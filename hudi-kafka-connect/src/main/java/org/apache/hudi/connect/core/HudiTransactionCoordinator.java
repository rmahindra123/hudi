/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.connect.core;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.connect.kafka.HudiKafkaControlAgent;
import org.apache.hudi.connect.writers.HudiCowWriter;
import org.apache.hudi.connect.writers.LocalCommitFile;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the Coordinator that
 * coordinates the Hudi write transactions
 * across all the Kafka partitions.
 */
public class HudiTransactionCoordinator implements TransactionCoordinator, Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(HudiTransactionCoordinator.class);
  private static final int START_COMMIT_INIT_DELAY_MS = 100;
  private static final int COMMIT_INTERVAL_MINS = 1;
  private static final int WRITE_STATUS_TIMEOUT_SECS = 30;

  private final String taskId;
  private final TopicPartition partition;
  private final ScheduledExecutorService scheduler;
  private final HudiKafkaControlAgent kafkaControlClient;
  private final BlockingQueue<CoordinatorEvent> events;
  private final ExecutorService executorService;
  private final HudiCowWriter hudiCowWriter;

  private String currentCommitTime;
  private int numPartitions;
  private Set<Integer> partitionsWriteStatusReceived;
  private Map<Integer, Long> globalCommittedKafkaOffsets;
  private Map<Integer, Long> currentConsumedKafkaOffsets;
  private State currentState;

  public HudiTransactionCoordinator(String taskId, TopicPartition partition, int numPartitions, HudiKafkaControlAgent kafkaControlClient) {
    this.taskId = taskId;
    this.partition = partition;
    this.numPartitions = numPartitions;
    this.kafkaControlClient = kafkaControlClient;
    this.events = new LinkedBlockingQueue<>();
    this.executorService = Executors.newSingleThreadExecutor();
    this.scheduler = Executors.newSingleThreadScheduledExecutor();
    this.currentCommitTime = StringUtils.EMPTY_STRING;
    this.partitionsWriteStatusReceived = new HashSet<>();
    this.globalCommittedKafkaOffsets = new HashMap<>();
    this.currentConsumedKafkaOffsets = new HashMap<>();
    this.currentState = State.INIT;
    this.hudiCowWriter = new HudiCowWriter(-1, true);
  }

  @Override
  public void start() {
    // Read the globalKafkaOffsets from the last commit file
    kafkaControlClient.registerTransactionCoordinator(this);
    initializeGlobalCommittedKafkaOffsets();
    // Submit the first start commit
    scheduler.schedule(() -> {
      events.add(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT, StringUtils.EMPTY_STRING));
    }, START_COMMIT_INIT_DELAY_MS, TimeUnit.MILLISECONDS);
    executorService.submit(this);
  }

  @Override
  public void stop() {
    kafkaControlClient.deregisterTransactionCoordinator(this);
    scheduler.shutdownNow();
    if (executorService != null) {
      boolean terminated = false;
      try {
        LOG.info("Shutting down Partition Leader executor service.");
        executorService.shutdown();
        LOG.info("Awaiting termination.");
        terminated = executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        // ignored
      }

      if (!terminated) {
        LOG.warn(
            "Unclean Kafka Control Manager executor service shutdown ");
        executorService.shutdownNow();
      }
    }
  }

  @Override
  public TopicPartition getPartition() {
    return partition;
  }

  @Override
  public void publishControlEvent(ControlEvent message) {
    CoordinatorEvent.CoordinatorEventType type;
    if (message.getMsgType().equals(ControlEvent.MsgType.WRITE_STATUS)) {
      type = CoordinatorEvent.CoordinatorEventType.WRITE_STATUS;
    } else {
      LOG.warn("The Coordinator should not be receiving messages of type {}", message.getMsgType().name());
      return;
    }
    CoordinatorEvent event = new CoordinatorEvent(type, message.getCommitTime());
    event.setMessage(message);
    events.add(event);
  }

  @Override
  public void run() {
    while (true) {
      try {
        CoordinatorEvent event = events.poll(100, TimeUnit.MILLISECONDS);
        // Ignore NULL and STALE events, unless its one to start a new COMMIT
        if (event == null
            || (!event.getEventType().equals(CoordinatorEvent.CoordinatorEventType.START_COMMIT)
            && (event.getCommitTime() != currentCommitTime))) {
          continue;
        }

        switch (event.getEventType()) {
          case START_COMMIT:
            startNewCommit();
            break;
          case END_COMMIT:
            endExistingCommit();
            break;
          case WRITE_STATUS:
            // Ignore stale write_status messages sent after
            if (event.getMessage() != null
                && currentState.equals(State.ENDED_COMMIT)) {
              onReceiveWriteStatus(event.getMessage());
            } else {
              LOG.warn("Could not process WRITE_STATUS due to missing message");
            }
            break;
          case ACK_COMMIT:
            submitAckCommit();
            break;
          case WRITE_STATUS_TIMEOUT:
            handleWriteStatusTimeout();
            break;
          default:
            throw new IllegalStateException("Partition Coordinator has received an illegal event type " + event.getEventType().name());
        }
      } catch (Exception exception) {
        LOG.warn("Error recieved while polling the event loop in Partition Coordinator", exception);
      }
    }
  }

  private void startNewCommit() {
    partitionsWriteStatusReceived.clear();
    currentCommitTime = hudiCowWriter.startCommit();
    ControlEvent message = new ControlEvent.Builder(
        ControlEvent.MsgType.START_COMMIT,
        currentCommitTime,
        partition.partition())
        .setCoodinatorInfo(
            new ControlEvent.CoordinatorInfo(globalCommittedKafkaOffsets))
        .build();
    kafkaControlClient.publishMessage(message);
    currentState = State.STARTED_COMMIT;
    // schedule a timeout for ending the current commit
    scheduler.schedule(() -> {
      events.add(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.END_COMMIT, currentCommitTime));
    }, COMMIT_INTERVAL_MINS, TimeUnit.MINUTES);
  }

  private void endExistingCommit() {
    ControlEvent message = new ControlEvent.Builder(
        ControlEvent.MsgType.END_COMMIT,
        currentCommitTime,
        partition.partition())
        .setCoodinatorInfo(
            new ControlEvent.CoordinatorInfo(globalCommittedKafkaOffsets))
        .build();
    currentConsumedKafkaOffsets.clear();
    kafkaControlClient.publishMessage(message);
    currentState = State.ENDED_COMMIT;

    // schedule a timeout for receiving all write statuses
    scheduler.schedule(() -> {
      events.add(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.WRITE_STATUS_TIMEOUT, currentCommitTime));
    }, WRITE_STATUS_TIMEOUT_SECS, TimeUnit.SECONDS);
  }

  private void onReceiveWriteStatus(ControlEvent message) {
    ControlEvent.ParticipantInfo participantInfo = message.getParticipantInfo();
    if (participantInfo.getOutcomeType().equals(ControlEvent.OutcomeType.WRITE_SUCCESS)) {
      int partition = message.getSenderPartition();
      partitionsWriteStatusReceived.add(partition);
      currentConsumedKafkaOffsets.put(partition, participantInfo.getKafkaCommitOffset());
    }
    if (partitionsWriteStatusReceived.size() >= numPartitions
        && currentState.equals(State.ENDED_COMMIT)) {
      // Commit the kafka offsets to the commit file
      try {
        commitFile(currentConsumedKafkaOffsets);
        globalCommittedKafkaOffsets.putAll(currentConsumedKafkaOffsets);
        currentState = State.WRITE_STATUS_RCVD;
        events.add(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.ACK_COMMIT, currentCommitTime));
      } catch (Exception exception) {
        LOG.error("Fatal error while committing file", exception);
      }
    }
  }

  private void handleWriteStatusTimeout() {
    // If we are still stuck in ENDED_STATE
    if (currentState.equals(State.ENDED_COMMIT)) {
      currentState = State.WRITE_STATUS_TIMEDOUT;
      LOG.warn("Did not receive the Write Status from all partitions");
      // Submit the next start commit
      scheduler.schedule(() -> {
        events.add(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT, StringUtils.EMPTY_STRING));
      }, 100, TimeUnit.MILLISECONDS);
    }
  }

  private void submitAckCommit() {
    ControlEvent message = new ControlEvent.Builder(
        ControlEvent.MsgType.ACK_COMMIT,
        currentCommitTime,
        partition.partition())
        .setCoodinatorInfo(
            new ControlEvent.CoordinatorInfo(globalCommittedKafkaOffsets))
        .build();
    kafkaControlClient.publishMessage(message);
    currentState = State.ACKED_COMMIT;

    // Submit the next start commit
    scheduler.schedule(() -> {
      events.add(new CoordinatorEvent(CoordinatorEvent.CoordinatorEventType.START_COMMIT, StringUtils.EMPTY_STRING));
    }, 100, TimeUnit.MILLISECONDS);
  }

  private void commitFile(Map<Integer, Long> committedKafkaOffsets) throws Exception {
    LocalCommitFile commitFile = new LocalCommitFile(currentCommitTime,
        numPartitions,
        new ArrayList<>(),
        partition.topic(),
        committedKafkaOffsets);
    commitFile.doCommit();
  }

  private void initializeGlobalCommittedKafkaOffsets() {
    try {
      LocalCommitFile latestCommitFile = LocalCommitFile.getLatestCommit();
      globalCommittedKafkaOffsets = latestCommitFile.getKafkaOffsets();
    } catch (Exception exception) {
      LOG.error("Fatal error fetching latest commit file", exception);
    }
  }

  private enum State {
    INIT,
    STARTED_COMMIT,
    ENDED_COMMIT,
    WRITE_STATUS_RCVD,
    WRITE_STATUS_TIMEDOUT,
    ACKED_COMMIT,
  }
}

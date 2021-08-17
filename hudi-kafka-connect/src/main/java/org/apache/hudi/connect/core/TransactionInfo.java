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

import org.apache.hudi.connect.writers.RecordWriter;
import org.apache.hudi.connect.writers.WriteStatus;

public class TransactionInfo {

  private final long commitTime;
  private final WriteStatus writeStatus;
  private final RecordWriter writer;
  private long lastWrittenKafkaOffset;
  private boolean commitInitiated;

  public TransactionInfo(long commitTime, WriteStatus writeStatus, RecordWriter writer) {
    this.commitTime = commitTime;
    this.writeStatus = writeStatus;
    this.writer = writer;
    this.lastWrittenKafkaOffset = 0;
    this.commitInitiated = false;
  }

  public long getCommitTime() {
    return commitTime;
  }

  public WriteStatus getWriteStatus() {
    return writeStatus;
  }

  public RecordWriter getWriter() {
    return writer;
  }

  public long getLastWrittenKafkaOffset() {
    return lastWrittenKafkaOffset;
  }

  public boolean isCommitInitiated() {
    return commitInitiated;
  }

  public void setLastWrittenKafkaOffset(long lastWrittenKafkaOffset) {
    this.lastWrittenKafkaOffset = lastWrittenKafkaOffset;
  }

  public void commitInitiated() {
    this.commitInitiated = true;
  }
}

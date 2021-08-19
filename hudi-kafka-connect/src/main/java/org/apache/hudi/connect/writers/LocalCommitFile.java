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

package org.apache.hudi.connect.writers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LocalCommitFile implements Serializable {

  public static final String BASE_PATH = "/tmp";
  private static final String COMMIT_FILE_PREFIX = ".hudi";
  private static final String COMMIT_FILE_EXT = "commit";
  private static final Logger LOG = LoggerFactory.getLogger(LocalCommitFile.class);

  private final String commitTime;
  private final int numPartitions;
  private final List<String> filesWritten;
  private final Map<Integer, Long> kafkaOffsets;
  private final String filepath;

  public LocalCommitFile(String commitTime, int numPartitions, List<String> filesWritten, String topicName, Map<Integer, Long> kafkaOffsets) {
    this.commitTime = commitTime;
    this.numPartitions = numPartitions;
    this.filesWritten = filesWritten;
    this.kafkaOffsets = kafkaOffsets;
    File commitFile = new File(BASE_PATH,
        String.format("%s-%s-%s.%s", COMMIT_FILE_PREFIX, commitTime, topicName, COMMIT_FILE_EXT));
    filepath = commitFile.getPath();
  }

  public String getCommitTime() {
    return commitTime;
  }

  public int getNumPartitions() {
    return numPartitions;
  }

  public List<String> getFilesWritten() {
    return filesWritten;
  }

  public Map<Integer, Long> getKafkaOffsets() {
    return kafkaOffsets;
  }

  @Override
  public String toString() {
    return " Commit Time: " + this.commitTime
        + " Number of Partitions : " + this.numPartitions
        + " Files Written : " + String.join(",", this.filesWritten)
        + " Kafka Offsets: " + this.kafkaOffsets.entrySet().stream()
        .map(entry -> entry.getKey() + ":" + entry.getValue())
        .collect(Collectors.joining(", ", "{", "}"));
  }

  public void doCommit() throws Exception {
    FileOutputStream fileOut = new FileOutputStream(filepath);
    ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
    objectOut.writeObject(this);
    objectOut.close();
    LOG.info("The file with path {} and commit time {} was written", filepath, commitTime);
  }

  public static LocalCommitFile getLatestCommit() throws IOException, ClassNotFoundException {
    DirectoryStream<Path> directoryStream = listCommitFiles();
    Path latestCommitFilePath = null;
    long maxCommitTime = -1;
    for (Path path : directoryStream) {
      String[] pathComponents = path.toString().split("-");
      long commitTime = Long.parseLong(pathComponents[1]);
      if (commitTime > maxCommitTime) {
        maxCommitTime = commitTime;
        latestCommitFilePath = path;
      }
    }
    if (latestCommitFilePath == null) {
      throw new IOException("Error finding the commit file with the latest commit");
    }
    FileInputStream fileIn = new FileInputStream(latestCommitFilePath.toString());
    ObjectInputStream objectIn = new ObjectInputStream(fileIn);
    LocalCommitFile latestCommitFile = (LocalCommitFile) objectIn.readObject();
    objectIn.close();
    LOG.info("Leader will use existing kafka offsets {} commited to the latest commit file {} ",
        latestCommitFile.getKafkaOffsets(),
        latestCommitFilePath.toString());
    return latestCommitFile;
  }

  private static DirectoryStream<Path> listCommitFiles() throws IOException {
    return Files.newDirectoryStream(
        Paths.get(BASE_PATH), COMMIT_FILE_PREFIX + "*" + COMMIT_FILE_EXT);
  }

}

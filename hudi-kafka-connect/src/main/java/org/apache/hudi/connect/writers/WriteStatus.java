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

import java.util.List;

public class WriteStatus {

  private final String commitTime;
  private int numRecords;
  private List<String> fileNames;
  private boolean isSuccess;

  public WriteStatus(String commitTime) {
    this.commitTime = commitTime;
    this.numRecords = 0;
  }

  public WriteStatus incrementRecordsWritten() {
    this.numRecords++;
    return this;
  }

  public void setNumRecords(int numRecords) {
    this.numRecords = numRecords;
  }

  public void setFileNames(List<String> fileNames) {
    this.fileNames = fileNames;
  }

  public void setSuccess(boolean success) {
    isSuccess = success;
  }

  public String getCommitTime() {
    return commitTime;
  }

  public int getNumRecords() {
    return numRecords;
  }

  public List<String> getFileNames() {
    return fileNames;
  }

  public boolean isSuccess() {
    return isSuccess;
  }
}

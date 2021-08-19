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

import org.apache.hudi.client.WriteStatus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;

public class SimpleFileWriter implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(SimpleFileWriter.class);
  public static final String BASE_PATH = "/tmp";
  private static final String FILE_EXTENSION = "log";
  private static final int NEWLINE_ASCII = 10;

  private final FileOutputStream fos;
  private final TopicPartition partition;
  private HudiCowWriter cowWriter;

  public SimpleFileWriter(TopicPartition partition, String commitTime) throws FileNotFoundException {
    File file = new File(BASE_PATH,
        String.format("%s-%s.%s", partition, commitTime, FILE_EXTENSION));
    fos = new FileOutputStream(file.getPath(), true);
    this.partition = partition;
    cowWriter = new HudiCowWriter(partition.partition(),false);
  }

  @Override
  public void write(SinkRecord record, String commitTime) throws IOException {
    fos.write(new ObjectMapper().writeValueAsBytes(record.value()));
    fos.write(NEWLINE_ASCII);

    if (cowWriter != null) {
      try {
        cowWriter.write(record, commitTime);
      } catch (Exception exception) {
        LOG.error("WNI HUDI OMG ERROR WRITING RECORD", exception);
      }
    }
  }

  @Override
  public List<WriteStatus> getWriteStatuses() {
    return cowWriter.getWriteStatuses();
  }

  public TopicPartition topicPartition() {
    return partition;
  }

  @Override
  public void close() throws IOException  {
    fos.flush();
    fos.close();
  }

}

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

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class HudiCowWriter implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HudiCowWriter.class);
  private static final String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\",\"name\": \"triprec\",\"fields\": [ "
      + "{\"name\": \"ts\",\"type\": \"long\"},{\"name\": \"uuid\", \"type\": \"string\"},"
      + "{\"name\": \"rider\", \"type\": \"string\"},{\"name\": \"driver\", \"type\": \"string\"},"
      + "{\"name\": \"begin_lat\", \"type\": \"double\"},{\"name\": \"begin_lon\", \"type\": \"double\"},"
      + "{\"name\": \"end_lat\", \"type\": \"double\"},{\"name\": \"end_lon\", \"type\": \"double\"},"
      + "{\"name\":\"fare\",\"type\": \"double\"}]}";

  private HoodieJavaWriteClient hoodieJavaWriteClient;

  public HudiCowWriter() {
    String tablePath = "file:///tmp/hoodie/sample-table";
    String tableName = "hoodie_rt";
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
    // initialize the table, if not done already
    Path path = new Path(tablePath);
    try {
      FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
      if (!fs.exists(path)) {
        LOG.error("WNI YES");
        HoodieTableMetaClient.withPropertyBuilder()
            .setTableType(HoodieTableType.COPY_ON_WRITE.name())
            .setTableName(tableName)
            .setPayloadClassName(HoodieAvroPayload.class.getName())
            .initTable(hadoopConf, tablePath);
      }

      // Create the write client to write some records in
      HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(tablePath)
          .withSchema(TRIP_EXAMPLE_SCHEMA)
          .withParallelism(2, 2)
          .withDeleteParallelism(2).forTable(tableName)
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
      hoodieJavaWriteClient =
          new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg);

      // inserts
      String newCommitTime = hoodieJavaWriteClient.startCommit();
      LOG.info("Starting commit " + newCommitTime);
    } catch (Exception exc) {
      LOG.error("WNI WNI OMG OMG ", exc);
    }
  }

  @Override
  public void write(SinkRecord record) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
}

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

import org.apache.hudi.avro.MercifulJsonConverter;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class HudiCowWriter implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HudiCowWriter.class);
  private static final String TABLE_PATH = "file:///tmp/hoodie/sample-table";
  private static final String TABLE_NAME = "hoodie_rt";
  private static final String SCHEMA = "{\n" +
      "  \"name\": \"MyClass\",\n" +
      "  \"type\": \"record\",\n" +
      "  \"namespace\": \"com.acme.avro\",\n" +
      "  \"fields\": [\n" +
      "    {\n" +
      "      \"name\": \"volume\",\n" +
      "      \"type\": \"int\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"symbol\",\n" +
      "      \"type\": \"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"ts\",\n" +
      "      \"type\": \"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"month\",\n" +
      "      \"type\": \"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"high\",\n" +
      "      \"type\": \"float\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"low\",\n" +
      "      \"type\": \"float\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"key\",\n" +
      "      \"type\": \"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"year\",\n" +
      "      \"type\": \"int\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"date\",\n" +
      "      \"type\": \"string\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"close\",\n" +
      "      \"type\": \"float\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"open\",\n" +
      "      \"type\": \"float\"\n" +
      "    },\n" +
      "    {\n" +
      "      \"name\": \"day\",\n" +
      "      \"type\": \"string\"\n" +
      "    }\n" +
      "  ]\n" +
      "}";

  private final JsonConverter jsonConverter;
  private final ObjectMapper mapper;
  private HoodieJavaWriteClient hoodieJavaWriteClient;

  public HudiCowWriter(int partition, boolean initTable) {
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    if (initTable) {
      try {
        // initialize the table, if not done already
        Path path = new Path(TABLE_PATH);
        FileSystem fs = FSUtils.getFs(TABLE_PATH, hadoopConf);
        if (!fs.exists(path)) {
          LOG.error("WNI YES");
          HoodieTableMetaClient.withPropertyBuilder()
              .setTableType(HoodieTableType.COPY_ON_WRITE.name())
              .setTableName(TABLE_NAME)
              .setPayloadClassName(HoodieAvroPayload.class.getName())
              .initTable(hadoopConf, TABLE_PATH);
        }
      } catch (Exception exception) {
        LOG.error("Fatal error initializing Table", exception);
      }
    }

    try {
      // Create the write client to write some records in
      HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder().withPath(TABLE_PATH)
          .withSchema(SCHEMA)
          .withParallelism(2, 2).withDeleteParallelism(2)
          .withAutoCommit(false)
          .forTable(TABLE_NAME)
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();
      HoodieJavaEngineContext context = new HoodieJavaEngineContext(hadoopConf);
      context.setKafkaPartition(String.valueOf(partition));
      hoodieJavaWriteClient =
          new HoodieJavaWriteClient<>(context, cfg);
    } catch (Exception exc) {
      LOG.error("WNI WNI OMG ", exc);
    }

    jsonConverter = new JsonConverter();
    Map<String, Object> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "false");
    jsonConverter.configure(converterConfig, false);
    mapper = new ObjectMapper();
  }

  public String startCommit() {
    String newCommitTime = hoodieJavaWriteClient.startCommit();
    LOG.info("WNI Starting commit " + newCommitTime);
    return newCommitTime;
  }

  public void endCommit(String commitTime) {
    LOG.info("WNI Ending commit " + commitTime);
    //hoodieJavaWriteClient.commit();
  }

  @Override
  public void write(SinkRecord record, String commitTime) throws IOException {
    String partitionPath = record.topic();
    MercifulJsonConverter converter = new MercifulJsonConverter();
    String jsonRecord = mapper.writeValueAsString(record.value());

    Schema.Parser parser = new Schema.Parser();
    GenericRecord avroRecord =
        converter.convert(jsonRecord, parser.parse(SCHEMA));

    HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
    HoodieRecord hudiRecord = new HoodieRecord(key,
        new HoodieAvroPayload(Option.of(avroRecord)));

    hoodieJavaWriteClient.insertPreppedRecords(Collections.singletonList(hudiRecord), commitTime);
  }

  @Override
  public void close() throws IOException {
    hoodieJavaWriteClient.close();
  }
}

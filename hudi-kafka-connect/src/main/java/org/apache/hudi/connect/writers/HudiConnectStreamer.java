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
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.schema.SchemaProvider;
import org.apache.hudi.table.action.commit.JavaUpsertPartitioner;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

public class HudiConnectStreamer implements RecordWriter {

  private static final Logger LOG = LoggerFactory.getLogger(HudiConnectStreamer.class);

  private final HudiConnectConfigs connectConfigs;
  private final SchemaProvider schemaProvider;
  private final List<WriteStatus> writeStatuses;
  private final ObjectMapper mapper;
  private HoodieJavaWriteClient hoodieJavaWriteClient;

  public HudiConnectStreamer(
      HudiConnectConfigs connectConfigs,
      int partition,
      boolean initTable) throws IOException {
    this.connectConfigs = connectConfigs;
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    try {
      this.schemaProvider = StringUtils.isNullOrEmpty(connectConfigs.getSchemaProviderClass()) ? null
          : (SchemaProvider) ReflectionUtils.loadClass(connectConfigs.getSchemaProviderClass(),
          new TypedProperties(connectConfigs.getProps()));

      // Create the write client to write some records in
      Properties props = new Properties();
      HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
          .withProperties(connectConfigs.getProps())
          .withProps(Collections.singletonMap(JavaUpsertPartitioner.NEW_FILE_ID_PREFIX_KEY, String.valueOf(partition)))
          .withSchema(schemaProvider.getSourceSchema().toString())
          .withParallelism(2, 2).withDeleteParallelism(2)
          .withAutoCommit(false)
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();

      if (initTable) {
        String tablePath = writeConfig.getBasePath();
        // initialize the table, if not done already
        Path path = new Path(tablePath);
        FileSystem fs = FSUtils.getFs(tablePath, hadoopConf);
        if (!fs.exists(path)) {
          HoodieTableMetaClient.withPropertyBuilder()
              .setTableType(HoodieTableType.COPY_ON_WRITE.name())
              .setTableName(writeConfig.getTableName())
              .setPayloadClassName(HoodieAvroPayload.class.getName())
              .initTable(hadoopConf, tablePath);
        }
      }

      hoodieJavaWriteClient =
          new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), writeConfig);

      this.writeStatuses = new ArrayList<>();

      JsonConverter jsonConverter = new JsonConverter();
      Map<String, Object> converterConfig = new HashMap<>();
      converterConfig.put("schemas.enable", "false");
      jsonConverter.configure(converterConfig, false);
      mapper = new ObjectMapper();
    } catch (Throwable e) {
      throw new IOException("Could not instantiate HudiConnectStreamer " + connectConfigs.getSchemaProviderClass(), e);
    }
  }

  public String startCommit() {
    String newCommitTime = hoodieJavaWriteClient.startCommit();
    LOG.info("WNI Starting commit " + newCommitTime);
    return newCommitTime;
  }

  public void endCommit(String commitTime, List<WriteStatus> writeStatuses) {
    LOG.info("WNI Ending commit " + commitTime);
    hoodieJavaWriteClient.commit(commitTime, writeStatuses, Option.empty(),
        HoodieActiveTimeline.COMMIT_ACTION, Collections.emptyMap());
  }

  @Override
  public void write(SinkRecord record, String commitTime) throws IOException {
    AvroConvertor convertor = new AvroConvertor(schemaProvider.getSourceSchema());
    Option<GenericRecord> avroRecord;
    switch (connectConfigs.getKafkaValueConverter()) {
      case "io.confluent.connect.avro.AvroConverter":
        avroRecord = Option.of((GenericRecord) record.value());
        break;
      case "org.apache.kafka.connect.json.JsonConverter":
        avroRecord = Option.of(convertor.fromJson(mapper.writeValueAsString(record.value())));
        break;
      case "org.apache.kafka.connect.storage.StringConverter":
        avroRecord = Option.of(convertor.fromJson((String) record.value()));
        break;
      default:
        throw new IOException("Unsupported Kafka Format type (" + connectConfigs.getKafkaValueConverter() + ")");
    }

    Random rand = new Random();
    int random = rand.nextInt(5);
    String partitionPath = "2020/01/01";
    switch (random) {
      case 1:
        partitionPath = "2020/01/01";
        break;
      case 2:
        partitionPath = "2020/02/01";
        break;
      case 3:
        partitionPath = "2020/03/01";
        break;
      case 4:
        partitionPath = "2020/04/01";
        break;
      case 5:
      default:
        partitionPath = "2020/05/01";
        break;
    }

    HoodieKey key = new HoodieKey(UUID.randomUUID().toString(), partitionPath);
    HoodieRecord hudiRecord = new HoodieRecord(key, new HoodieAvroPayload(avroRecord));
    List<WriteStatus> hudiWriteStatus = hoodieJavaWriteClient.insertPreppedRecords(Collections.singletonList(hudiRecord), commitTime);
    writeStatuses.addAll(hudiWriteStatus);
  }

  @Override
  public List<WriteStatus> getWriteStatuses() {
    return writeStatuses;
  }

  @Override
  public void close() throws IOException {
    hoodieJavaWriteClient.close();
  }
}

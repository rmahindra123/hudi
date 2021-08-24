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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieWriterUtils;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
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
import org.apache.hudi.fileid.KafkaConnectFileIdPrefixProvider;
import org.apache.hudi.fileid.RandomFileIdPrefixProvider;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HiveSyncTool;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.keygen.factory.HoodieAvroKeyGeneratorFactory;
import org.apache.hudi.schema.SchemaProvider;
import org.apache.hudi.sync.common.AbstractSyncTool;
import org.apache.hudi.utilities.sources.helpers.AvroConvertor;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

public class HudiConnectStreamer {

  private static final Logger LOG = LoggerFactory.getLogger(HudiConnectStreamer.class);
  private static final String TABLE_FORMAT = "PARQUET";

  private final HudiConnectConfigs connectConfigs;
  /**
   * Extract the key for the target table.
   */
  private final KeyGenerator keyGenerator;
  private final SchemaProvider schemaProvider;
  private final String tableBasePath;
  private final String tableName;
  private final FileSystem fs;
  private final HoodieEngineContext context;
  private final HoodieWriteConfig writeConfig;
  private final HoodieJavaWriteClient hoodieJavaWriteClient;

  public HudiConnectStreamer(
      HudiConnectConfigs connectConfigs,
      TopicPartition partition,
      boolean initTable) throws IOException {
    this.connectConfigs = connectConfigs;
    Configuration hadoopConf = new Configuration();
    hadoopConf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

    try {
      this.schemaProvider = StringUtils.isNullOrEmpty(connectConfigs.getSchemaProviderClass()) ? null
          : (SchemaProvider) ReflectionUtils.loadClass(connectConfigs.getSchemaProviderClass(),
          new TypedProperties(connectConfigs.getProps()));

      this.keyGenerator = HoodieAvroKeyGeneratorFactory.createKeyGenerator(
          new TypedProperties(connectConfigs.getProps()));

      // Create the write client to write some records in
      writeConfig = HoodieWriteConfig.newBuilder()
          .withProperties(connectConfigs.getProps())
          //.withFileIdPrefixProviderClassName(KafkaConnectFileIdPrefixProvider.class.getName())
          .withFileIdPrefixProviderClassName(RandomFileIdPrefixProvider.class.getName())
          .withProps(Collections.singletonMap(
              KafkaConnectFileIdPrefixProvider.KAFKA_CONNECT_PARTITION_ID,
              String.valueOf(partition.partition())))
          .withSchema(schemaProvider.getSourceSchema().toString())
          .withParallelism(2, 2).withDeleteParallelism(2)
          .withAutoCommit(false)
          .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(HoodieIndex.IndexType.INMEMORY).build())
          .withCompactionConfig(HoodieCompactionConfig.newBuilder().archiveCommitsWith(20, 30).build()).build();

      tableBasePath = writeConfig.getBasePath();
      tableName = writeConfig.getTableName();
      fs = FSUtils.getFs(tableBasePath, hadoopConf);
      if (initTable) {
        // initialize the table, if not done already
        Path path = new Path(tableBasePath);
        String partitionColumns = HoodieWriterUtils.getPartitionColumns(keyGenerator);
        if (!fs.exists(path)) {
          HoodieTableMetaClient.withPropertyBuilder()
              .setTableType(HoodieTableType.COPY_ON_WRITE.name())
              .setTableName(tableName)
              .setPayloadClassName(HoodieAvroPayload.class.getName())
              //.setBaseFileFormat()
              .setPartitionFields(partitionColumns)
              //.setRecordKeyFields()
              .setKeyGeneratorClassProp(writeConfig.getKeyGeneratorClass())
              //.setPreCombineField()
              .initTable(hadoopConf, tableBasePath);
        }
      }

      context = new HoodieJavaEngineContext(hadoopConf);
      hoodieJavaWriteClient = new HoodieJavaWriteClient<>(context, writeConfig);

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
    syncMeta();
  }

  public Writer newTransactionWriter(String commitTime) {
    return new Writer(
        context,
        hoodieJavaWriteClient,
        commitTime,
        connectConfigs,
        writeConfig,
        keyGenerator,
        schemaProvider);
  }

  public void syncMeta() {
    Set<String> syncClientToolClasses = new HashSet<>(
        Arrays.asList(connectConfigs.getMetaSyncClasses().split(",")));
    if (connectConfigs.isMetaSyncEnabled()) {
      for (String impl : syncClientToolClasses) {
        impl = impl.trim();
        switch (impl) {
          case "org.apache.hudi.hive.HiveSyncTool":
            syncHive();
            break;
          default:
            FileSystem fs = FSUtils.getFs(tableBasePath, new Configuration());
            Properties properties = new Properties();
            properties.putAll(connectConfigs.getProps());
            properties.put("basePath", tableBasePath);
            properties.put("baseFileFormat", TABLE_FORMAT);
            AbstractSyncTool syncTool = (AbstractSyncTool) ReflectionUtils.loadClass(impl, new Class[]{Properties.class, FileSystem.class}, properties, fs);
            syncTool.syncHoodieTable();
        }
      }
    }
  }

  private void syncHive() {
    HiveSyncConfig hiveSyncConfig = DataSourceUtils.buildHiveSyncConfig(
        new TypedProperties(connectConfigs.getProps()),
        tableBasePath,
        TABLE_FORMAT);
    LOG.info("Syncing target hoodie table with hive table("
        + hiveSyncConfig.tableName
        + "). Hive metastore URL :"
        + hiveSyncConfig.jdbcUrl
        + ", basePath :" + tableBasePath);
    LOG.info("Hive Sync Conf => " + hiveSyncConfig.toString());
    HiveConf hiveConf = new HiveConf(fs.getConf(), HiveConf.class);
    LOG.info("Hive Conf => " + hiveConf.getAllProperties().toString());
    new HiveSyncTool(hiveSyncConfig, hiveConf, fs).syncHoodieTable();
  }

  public static class Writer implements RecordWriter {

    private final HudiConnectConfigs connectConfigs;
    private final HudiWriter<HoodieAvroPayload> hudiWriter;
    private final KeyGenerator keyGenerator;
    private final SchemaProvider schemaProvider;
    private final ObjectMapper mapper;

    public Writer(HoodieEngineContext context,
                  HoodieJavaWriteClient javaWriteClient,
                  String commitTime,
                  HudiConnectConfigs connectConfigs,
                  HoodieWriteConfig writeConfig,
                  KeyGenerator keyGenerator,
                  SchemaProvider schemaProvider) {
      this.connectConfigs = connectConfigs;
      this.hudiWriter = new HudiBufferedWriter<>(context, javaWriteClient, commitTime, writeConfig);
      this.keyGenerator = keyGenerator;
      this.schemaProvider = schemaProvider;
      this.mapper = new ObjectMapper();
    }

    @Override
    public void write(SinkRecord record) throws IOException {
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

      HoodieRecord hudiRecord = new HoodieRecord(keyGenerator.getKey(avroRecord.get()), new HoodieAvroPayload(avroRecord));
      hudiWriter.writeRecord(hudiRecord);
    }

    @Override
    public List<WriteStatus> close() throws IOException {
      return hudiWriter.close();
    }
  }
}

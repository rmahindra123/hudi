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

package org.apache.hudi.utilities.sources;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter;
import org.apache.hudi.utilities.schema.FilebasedSchemaProvider;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import static org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;

/**
 * Tests against {@link JsonKafkaSource}.
 */
public class TestJsonKafkaSource extends UtilitiesTestBase {

  private static String TEST_TOPIC_NAME = "hoodie_test";

  private FilebasedSchemaProvider flattedFilebasedSchemaProvider;
  private FilebasedSchemaProvider nestedFilebasedSchemaProvider;
  private KafkaTestUtils testUtils;
  private HoodieDeltaStreamerMetrics metrics = mock(HoodieDeltaStreamerMetrics.class);

  @BeforeAll
  public static void initClass() throws Exception {
    UtilitiesTestBase.initClass(false);
  }

  @AfterAll
  public static void cleanupClass() {
    UtilitiesTestBase.cleanupClass();
  }

  @BeforeEach
  public void setup() throws Exception {
    super.setup();
    flattedFilebasedSchemaProvider = new FilebasedSchemaProvider(Helpers.setupFlattenedSchemaOnDFS(), jsc);
    nestedFilebasedSchemaProvider = new FilebasedSchemaProvider(Helpers.setupSchemaOnDFS(), jsc);
    testUtils = new KafkaTestUtils();
    testUtils.setup();
  }

  @AfterEach
  public void teardown() throws Exception {
    super.teardown();
    testUtils.teardown();
  }

  private TypedProperties createPropsForJsonSource(Long maxEventsToReadFromKafkaSource, String resetStrategy) {
    TypedProperties props = new TypedProperties();
    props.setProperty("hoodie.deltastreamer.source.kafka.topic", TEST_TOPIC_NAME);
    props.setProperty("bootstrap.servers", testUtils.brokerAddress());
    props.setProperty("auto.offset.reset", resetStrategy);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents",
        maxEventsToReadFromKafkaSource != null ? String.valueOf(maxEventsToReadFromKafkaSource) :
            String.valueOf(Config.MAX_EVENTS_FROM_KAFKA_SOURCE_PROP.defaultValue()));
    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    return props;
  }

  @ParameterizedTest
  @MethodSource("testArguments")
  public void testJsonKafkaSource(String sourceClass, SCHEMA_TYPE schemaType) {
    // topic setup.
    SchemaProvider schemaProvider;// = isSchemaProvided ? filebasedSchemaProvider : null;

    switch (schemaType) {
      case NONE:
        schemaProvider = null;
        break;
      case FLATTENED:
        schemaProvider = flattedFilebasedSchemaProvider;
        break;
      case NESTED:
      default:
        schemaProvider = nestedFilebasedSchemaProvider;
        break;
    }

    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(null, "earliest");

    Source<?> jsonSource = (Source<?>) ReflectionUtils.loadClass(sourceClass, new Class[]{TypedProperties.class, JavaSparkContext.class,
            SparkSession.class, SchemaProvider.class, HoodieDeltaStreamerMetrics.class},
        props, jsc, sparkSession, schemaProvider, metrics);

    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    String[] messages = Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1, schemaType.equals(SCHEMA_TYPE.FLATTENED)));
    System.out.println("WNI MESS " + messages[0]);
    String message = "{\"timestamp\": 0, \"_row_key\": \"531cefe8-ccec-4746-a961-e63353b03b29\", \"partition_path\": \"2015/03/16\", \"rider\": \"rider-000\", \"driver\": \"driver-000\", \"begin_lat\": 0.18433112391820694, \"begin_lon\": 0.4457079093559174, \"end_lat\": 0.38128402026859787, \"end_lon\": 0.4528353922784837, \"distance_in_meters\": 806140026, \"seconds_since_epoch\": -606602876908456549, \"weight\": 0.34158283, \"current_date\": 18867, \"current_ts\": 1630134406124, \"city_to_state\": {\"LA\": \"CA\"}, \"_hoodie_is_deleted\": false}";
    testUtils.sendMessages(TEST_TOPIC_NAME, new String[] {message});
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertFalse(fetch1.getBatch().get().isEmpty());
    //assertEquals(900, fetch1.getBatch().get().count());
    // Test Avro To DataFrame<Row> path
    /*if (schemaProvider != null) {
      Dataset<Row> fetch1AsRows = AvroConversionUtils.createDataFrame(JavaRDD.toRDD(fetch1.getBatch().get()),
          schemaProvider.getSourceSchema().toString(), jsonSource.getSparkSession());
      assertEquals(900, fetch1AsRows.count());
    }*/

    // 2. Produce new data, extract new data
    /*testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(1100, fetch2.getBatch().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    InputBatch<JavaRDD<GenericRecord>> fetch3 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(fetch2.getBatch().get().count(), fetch3.getBatch().get().count());
    assertEquals(fetch2.getCheckpointForNextBatch(), fetch3.getCheckpointForNextBatch());
    // Same using Row API
    InputBatch<Dataset<Row>> fetch3AsRows =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(fetch2.getBatch().get().count(), fetch3AsRows.getBatch().get().count());
    assertEquals(fetch2.getCheckpointForNextBatch(), fetch3AsRows.getCheckpointForNextBatch());

    // 4. Extract with latest checkpoint => no new data returned
    InputBatch<JavaRDD<GenericRecord>> fetch4 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch2.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch4.getBatch());
    // Same using Row API
    InputBatch<Dataset<Row>> fetch4AsRows =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch2.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch4AsRows.getBatch());*/
  }

  // test case with kafka offset reset strategy
  /*@Test
  public void testJsonKafkaSourceResetStrategy() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();

    TypedProperties earliestProps = createPropsForJsonSource(null, "earliest");
    Source earliestJsonSource = new JsonKafkaSource(earliestProps, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter earliestKafkaSource = new SourceFormatAdapter(earliestJsonSource);

    TypedProperties latestProps = createPropsForJsonSource(null, "latest");
    Source latestJsonSource = new JsonKafkaSource(latestProps, jsc, sparkSession, schemaProvider, metrics);
    SourceFormatAdapter latestKafkaSource = new SourceFormatAdapter(latestJsonSource);

    // 1. Extract with a none data kafka checkpoint
    // => get a checkpoint string like "hoodie_test,0:0,1:0", latest checkpoint should be equals to earliest checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch0 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    InputBatch<JavaRDD<GenericRecord>> latFetch0 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch0.getBatch(), latFetch0.getBatch());
    assertEquals(earFetch0.getCheckpointForNextBatch(), latFetch0.getCheckpointForNextBatch());

    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    // 2. Extract new checkpoint with a null / empty string pre checkpoint
    // => earliest fetch with max source limit will get all of data and a end offset checkpoint
    InputBatch<JavaRDD<GenericRecord>> earFetch1 = earliestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);

    // => [a null pre checkpoint] latest reset fetch will get a end offset checkpoint same to earliest
    InputBatch<JavaRDD<GenericRecord>> latFetch1 = latestKafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(earFetch1.getCheckpointForNextBatch(), latFetch1.getCheckpointForNextBatch());
  }*/

  @Test
  public void testJsonKafkaSourceWithDefaultUpperCap() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(Long.MAX_VALUE, "earliest");

//Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    Source jsonSource = new JsonKafkaRowSource(props, jsc, sparkSession, nestedFilebasedSchemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    /*
    1. Extract without any checkpoint => get all the data, respecting default upper cap since both sourceLimit and
    maxEventsFromKafkaSourceProp are set to Long.MAX_VALUE
     */
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE);
    assertEquals(1000, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on sourceLimit
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 1500);
    assertEquals(1000, fetch2.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceInsertRecordsLessSourceLimit() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(Long.MAX_VALUE, "earliest");

//Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    Source jsonSource = new JsonKafkaRowSource(props, jsc, sparkSession, nestedFilebasedSchemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);
    props.setProperty("hoodie.deltastreamer.kafka.source.maxEvents", "500");

    /*
     1. maxEventsFromKafkaSourceProp set to more than generated insert records
     and sourceLimit less than the generated insert records num.
     */
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 400)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 300);
    assertEquals(300, fetch1.getBatch().get().count());

    /*
     2. Produce new data, extract new data based on sourceLimit
     and sourceLimit less than the generated insert records num.
     */
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 600)));
    InputBatch<Dataset<Row>> fetch2 =
            kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), 300);
    assertEquals(300, fetch2.getBatch().get().count());
  }

  @Test
  public void testJsonKafkaSourceWithConfigurableUpperCap() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(500L, "earliest");

//Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    Source jsonSource = new JsonKafkaRowSource(props, jsc, sparkSession, nestedFilebasedSchemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));
    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 900);
    assertEquals(900, fetch1.getBatch().get().count());

    // 2. Produce new data, extract new data based on upper cap
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 1000)));
    InputBatch<Dataset<Row>> fetch2 =
        kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(500, fetch2.getBatch().get().count());

    //fetch data respecting source limit where upper cap > sourceLimit
    InputBatch<JavaRDD<GenericRecord>> fetch3 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), 400);
    assertEquals(400, fetch3.getBatch().get().count());

    //fetch data respecting source limit where upper cap < sourceLimit
    InputBatch<JavaRDD<GenericRecord>> fetch4 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch2.getCheckpointForNextBatch()), 600);
    assertEquals(600, fetch4.getBatch().get().count());

    // 3. Extract with previous checkpoint => gives same data back (idempotent)
    InputBatch<JavaRDD<GenericRecord>> fetch5 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(fetch2.getBatch().get().count(), fetch5.getBatch().get().count());
    assertEquals(fetch2.getCheckpointForNextBatch(), fetch5.getCheckpointForNextBatch());

    // 4. Extract with latest checkpoint => no new data returned
    InputBatch<JavaRDD<GenericRecord>> fetch6 =
        kafkaSource.fetchNewDataInAvroFormat(Option.of(fetch4.getCheckpointForNextBatch()), Long.MAX_VALUE);
    assertEquals(Option.empty(), fetch6.getBatch());
  }

  @Test
  public void testCommitOffsetToKafka() {
    // topic setup.
    testUtils.createTopic(TEST_TOPIC_NAME, 2);
    List<TopicPartition> topicPartitions = new ArrayList<>();
    TopicPartition topicPartition0 = new TopicPartition(TEST_TOPIC_NAME, 0);
    topicPartitions.add(topicPartition0);
    TopicPartition topicPartition1 = new TopicPartition(TEST_TOPIC_NAME, 1);
    topicPartitions.add(topicPartition1);

    HoodieTestDataGenerator dataGenerator = new HoodieTestDataGenerator();
    TypedProperties props = createPropsForJsonSource(null, "earliest");
    props.put(ENABLE_KAFKA_COMMIT_OFFSET.key(), "true");
//Source jsonSource = new JsonKafkaSource(props, jsc, sparkSession, schemaProvider, metrics);
    Source jsonSource = new JsonKafkaRowSource(props, jsc, sparkSession, nestedFilebasedSchemaProvider, metrics);
    SourceFormatAdapter kafkaSource = new SourceFormatAdapter(jsonSource);

    // 1. Extract without any checkpoint => get all the data, respecting sourceLimit
    assertEquals(Option.empty(), kafkaSource.fetchNewDataInAvroFormat(Option.empty(), Long.MAX_VALUE).getBatch());
    testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("000", 1000)));

    InputBatch<JavaRDD<GenericRecord>> fetch1 = kafkaSource.fetchNewDataInAvroFormat(Option.empty(), 599);
    // commit to kafka after first batch
    kafkaSource.getSource().onCommit(fetch1.getCheckpointForNextBatch());
    try (KafkaConsumer consumer = new KafkaConsumer(props)) {
      consumer.assign(topicPartitions);

      OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition0);
      assertNotNull(offsetAndMetadata);
      assertEquals(300, offsetAndMetadata.offset());
      offsetAndMetadata = consumer.committed(topicPartition1);
      assertNotNull(offsetAndMetadata);
      assertEquals(299, offsetAndMetadata.offset());
      // end offsets will point to 500 for each partition because we consumed less messages from first batch
      Map endOffsets = consumer.endOffsets(topicPartitions);
      assertEquals(500L, endOffsets.get(topicPartition0));
      assertEquals(500L, endOffsets.get(topicPartition1));

      testUtils.sendMessages(TEST_TOPIC_NAME, Helpers.jsonifyRecords(dataGenerator.generateInserts("001", 500)));
      InputBatch<Dataset<Row>> fetch2 =
              kafkaSource.fetchNewDataInRowFormat(Option.of(fetch1.getCheckpointForNextBatch()), Long.MAX_VALUE);

      // commit to Kafka after second batch is processed completely
      kafkaSource.getSource().onCommit(fetch2.getCheckpointForNextBatch());

      offsetAndMetadata = consumer.committed(topicPartition0);
      assertNotNull(offsetAndMetadata);
      assertEquals(750, offsetAndMetadata.offset());
      offsetAndMetadata = consumer.committed(topicPartition1);
      assertNotNull(offsetAndMetadata);
      assertEquals(750, offsetAndMetadata.offset());

      endOffsets = consumer.endOffsets(topicPartitions);
      assertEquals(750L, endOffsets.get(topicPartition0));
      assertEquals(750L, endOffsets.get(topicPartition1));
    }
    // check failure case
    props.remove(ConsumerConfig.GROUP_ID_CONFIG);
    assertThrows(HoodieNotSupportedException.class,() -> kafkaSource.getSource().onCommit(""));
  }

  private static Stream<Arguments> testArguments() {
    // Arguments : 1. Kafka Source Type 2. type of Schema
    return Stream.of(
        arguments(JsonKafkaRowSource.class.getName(), SCHEMA_TYPE.NESTED)
        //arguments(JsonKafkaRowSource.class.getName(), SCHEMA_TYPE.NONE),
        //arguments(JsonKafkaRowSource.class.getName(), SCHEMA_TYPE.FLATTENED),
        //arguments(JsonKafkaSource.class.getName(), SCHEMA_TYPE.NESTED)
    );
  }

  private enum SCHEMA_TYPE {
    NONE,
    FLATTENED,
    NESTED
  }
}

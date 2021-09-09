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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;

import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;

import org.apache.hudi.AvroConversionUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import org.apache.hudi.utilities.sources.helpers.DebeziumConstants;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

/**
 * Debezium streaming source which expects change events as Kafka Avro records. This source should only be used
 * if a Debezium bootstrap source has been used first.
 *
 * @see DebeziumBootstrapSource
 * Reads avro serialized Kafka data, based on the confluent schema-registry. This multiplexes numerous topics with same schema as a single source.
 */
public class DebeziumSource extends RowSource {

  public static final String KAFKA_SCHEMA_REGISTRY_URL = "schema.registry.url";
  public static final String OVERRIDE_CHECKPOINT_STRING = "hoodie.debezium.override.initial.checkpoint.key";

  private static final Logger LOG = LogManager.getLogger(DebeziumSource.class);
  /**
   * Columns to whitelist.
   */
  private static final String DB_SELECTED_COLUMNS = "hoodie.source.jdbc.selected.columns";
  private static final String CONNECT_NAME_KEY = "connect.name";
  private static final String DATE_CONNECT_NAME = "custom.debezium.DateString";

  private final boolean isInitialCheckpoint;
  private final KafkaOffsetGen offsetGen;
  private final HoodieDeltaStreamerMetrics metrics;

  public DebeziumSource(TypedProperties props, JavaSparkContext sparkContext,
                        SparkSession sparkSession,
                        SchemaProvider schemaProvider,
                        HoodieDeltaStreamerMetrics metrics) {
    super(props, sparkContext, sparkSession, schemaProvider);
    props.put("key.deserializer", StringDeserializer.class);
    props.put("value.deserializer", KafkaAvroDeserializer.class);

    isInitialCheckpoint = props.getBoolean(DebeziumConstants.IS_INITIAL_CHECKPOINT_CONFIG_KEY, false);
    offsetGen = new KafkaOffsetGen(props);
    this.metrics = metrics;
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    String overrideCheckpointStr = props.getString(OVERRIDE_CHECKPOINT_STRING, "");

    // Adds a check to ensure streaming is never the first commit in a Hudi Table
    /*if (!lastCkptStr.isPresent() && !isInitialCheckpoint) {
      throw new HoodieException("Debezium streaming requires a previous kafka offset to begin from.");
    } else if (lastCkptStr.isPresent() && isInitialCheckpoint) {
      throw new HoodieException("There exists a previous commit, but this must be the first commit for the Hudi table.");
    } else if (!isInitialCheckpoint && !overrideCheckpointStr.isEmpty()) {
      throw new HoodieException("Cannot override checkpoint for commit that is not the first.");
    }*/

    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCkptStr, sourceLimit, metrics);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());

    if (totalNewMsgs == 0) {
      // If there are no new messages, use empty dataframe with no schema. This is because the schema from schema registry can only be considered
      // up to date if a change event has occured.
      return Pair.of(Option.of(sparkSession.emptyDataFrame()), overrideCheckpointStr.isEmpty() ? CheckpointUtils.offsetsToStr(offsetRanges) : overrideCheckpointStr);
    } else {
      String schemaStr = getSchema(offsetGen.getTopicName());
      Dataset<Row> dataset = toDataset(offsetRanges, offsetGen, schemaStr);
      dataset = convertDateColumns(dataset, new Schema.Parser().parse(schemaStr));
      LOG.info(String.format("Spark schema of Kafka Payload for topic %s:\n%s", offsetGen.getTopicName(), dataset.schema().treeString()));
      LOG.info(String.format("New checkpoint string: %s", CheckpointUtils.offsetsToStr(offsetRanges)));
      return Pair.of(Option.of(dataset), overrideCheckpointStr.isEmpty() ? CheckpointUtils.offsetsToStr(offsetRanges) : overrideCheckpointStr);
    }
  }

  /**
   * Gets the Avro schema for a Kafka topic from schema registry.
   *
   * @param topicName Topic name
   * @return Avro schema in String format
   */
  private String getSchema(String topicName) {
    String schemaUrl = String.format("%s/subjects/%s-value/versions/latest", props.getString(KAFKA_SCHEMA_REGISTRY_URL), topicName);
    try {
      URL registry = new URL(schemaUrl);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(registry.openStream());
      String schema = node.get("schema").asText();
      LOG.info(String.format("Reading schema from %s: %s", schemaUrl, schema));
      return schema;
    } catch (Exception e) {
      LOG.warn(String.format("Error reading from schema registry for url: %s, will instead use empty schema", schemaUrl), e);
      return getEmptySchema();
    }
  }

  /**
   * Converts a Kafka Topic offset into a Spark dataset.
   *
   * @param offsetRanges Offset ranges
   * @param offsetGen    KafkaOffsetGen
   * @return Spark dataset
   */
  private Dataset<Row> toDataset(OffsetRange[] offsetRanges, KafkaOffsetGen offsetGen, String schemaStr) {
    Dataset<Row> kafkaData = flattenDebeziumPayload(
        convertSparkArrayColumnsToString(
            AvroConversionUtils.createDataFrame(
                KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges, LocationStrategies.PreferConsistent())
                    .map(obj -> (GenericRecord) obj.value())
                    .rdd(), schemaStr,
                sparkSession
            )
        )
    );

    List<String> cols = props.getStringList(DB_SELECTED_COLUMNS, ",", Collections.emptyList());
    if (cols.size() > 0 && kafkaData.schema().fields().length > 0) {
      cols = Stream.concat(DebeziumConstants.META_COLUMNS.stream(), cols.stream()).collect(Collectors.toList());
      kafkaData = kafkaData.select(cols.stream().map(functions::col).toArray(Column[]::new));
    }

    return kafkaData;
  }

  /**
   * Debezium Kafka Payload has a nested structure (see https://debezium.io/documentation/reference/1.4/connectors/postgresql.html#postgresql-create-events).
   * This function flattens this nested structure for the Postgres data, and also extracts a subset of Debezium metadata fields.
   *
   * @param rowDataset Dataset containing Debezium Payloads
   * @return New dataset with flattened columns
   */
  private static Dataset<Row> flattenDebeziumPayload(Dataset<Row> rowDataset) {
    if (rowDataset.columns().length > 0) {
      // Only flatten for non-empty schemas
      Dataset<Row> insertedOrUpdatedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INP_OP_FIELD, DebeziumConstants.MODIFIED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_TS_MS_FIELD, DebeziumConstants.SOURCE_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_TXID_FIELD, DebeziumConstants.SOURCE_TX_ID_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_LSN_FIELD, DebeziumConstants.SOURCE_LSN_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_XMIN_FIELD, DebeziumConstants.SOURCE_XMIN_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_NAME_FIELD, DebeziumConstants.MODIFIED_SHARD_NAME),
              String.format("%s.*", DebeziumConstants.INP_AFTER_FIELD)
          )
          .filter(rowDataset.col("op").notEqual("d"));

      Dataset<Row> deletedData = rowDataset
          .selectExpr(
              String.format("%s as %s", DebeziumConstants.INP_OP_FIELD, DebeziumConstants.MODIFIED_OP_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_TS_MS_FIELD, DebeziumConstants.UPSTREAM_PROCESSING_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_TS_MS_FIELD, DebeziumConstants.SOURCE_TS_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_TXID_FIELD, DebeziumConstants.SOURCE_TX_ID_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_LSN_FIELD, DebeziumConstants.SOURCE_LSN_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_XMIN_FIELD, DebeziumConstants.SOURCE_XMIN_COL_NAME),
              String.format("%s as %s", DebeziumConstants.INP_QUALIFIED_SOURCE_NAME_FIELD, DebeziumConstants.MODIFIED_SHARD_NAME),
              String.format("%s.*", DebeziumConstants.INP_BEFORE_FIELD)
          )
          .filter(rowDataset.col("op").equalTo("d"));

      return insertedOrUpdatedData.union(deletedData);
    } else {
      return rowDataset;
    }
  }

  /**
   * Generates an Avro schema without any fields.
   *
   * @return Avro schema in String format
   */
  private static String getEmptySchema() {
    return SchemaBuilder.builder()
        .record("empty_record")
        .fields()
        .endRecord()
        .toString();
  }

  /**
   * Converts string formatted date columns into Spark date columns.
   *
   * @param dataset Spark dataset
   * @param schema  Avro schema from Debezium
   * @return Converted dataset
   */
  public static Dataset<Row> convertDateColumns(Dataset<Row> dataset, Schema schema) {
    if (schema.getField("before") != null) {
      List<String> dateFields = schema.getField("before")
          .schema()
          .getTypes()
          .get(1)
          .getFields()
          .stream()
          .filter(field -> {
            if (field.schema().getType() == Type.UNION) {
              return field.schema().getTypes().stream().anyMatch(
                  schemaInUnion -> DATE_CONNECT_NAME.equals(schemaInUnion.getProp(CONNECT_NAME_KEY))
              );
            } else {
              return DATE_CONNECT_NAME.equals(field.schema().getProp(CONNECT_NAME_KEY));
            }
          }).map(Field::name).collect(Collectors.toList());

      LOG.info("Date fields: " + dateFields.toString());

      for (String dateCol : dateFields) {
        dataset = dataset.withColumn(dateCol, functions.col(dateCol).cast(DataTypes.DateType));
      }
    }

    return dataset;
  }

  /**
   * Converts Array types to String types because not all Postgres array columns are supported to be converted
   * to Spark array columns.
   *
   * @param dataset Dataframe to modify
   * @return Modified dataframe
   */
  private static Dataset<Row> convertSparkArrayColumnsToString(Dataset<Row> dataset) {
    List<String> arrayColumns = Arrays.stream(dataset.schema().fields())
        .filter(field -> field.dataType().typeName().toLowerCase().startsWith("array"))
        .map(StructField::name)
        .collect(Collectors.toList());

    for (String colName : arrayColumns) {
      dataset = dataset.withColumn(colName, functions.col(colName).cast(DataTypes.StringType));
    }

    return dataset;
  }
}

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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ImmutablePair;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.helpers.KafkaOffsetGen;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.avro.SchemaConverters;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

/**
 * Read json kafka data into Spark Rows.
 */
public class JsonKafkaRowSource extends RowSource {

  private static final Logger LOG = LogManager.getLogger(JsonKafkaSource.class);

  private final KafkaOffsetGen offsetGen;

  private final HoodieDeltaStreamerMetrics metrics;
  private final StructType sourceSchema;

  public JsonKafkaRowSource(TypedProperties properties, JavaSparkContext sparkContext, SparkSession sparkSession,
                         SchemaProvider schemaProvider, HoodieDeltaStreamerMetrics metrics) {
    super(properties, sparkContext, sparkSession, schemaProvider);
    this.metrics = metrics;
    properties.put("key.deserializer", StringDeserializer.class);
    properties.put("value.deserializer", StringDeserializer.class);
    offsetGen = new KafkaOffsetGen(properties);
    if (schemaProvider != null) {
      sourceSchema = (StructType) SchemaConverters.toSqlType(schemaProvider.getSourceSchema())
          .dataType();
    } else {
      sourceSchema = null;
    }
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCkptStr, sourceLimit, metrics);
    long totalNewMsgs = KafkaOffsetGen.CheckpointUtils.totalNewMessages(offsetRanges);
    LOG.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    if (totalNewMsgs <= 0) {
      return new ImmutablePair<>(Option.empty(), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
    }
    JavaRDD<String> newDataRDD = toRDD(offsetRanges);
    DataFrameReader dataFrameReader = sparkSession.read().format("json");
    if (sourceSchema != null) {
      // Source schema is specified, pass it to the reader
      dataFrameReader.schema(sourceSchema);
    }
    dataFrameReader.option("inferSchema", Boolean.toString(sourceSchema == null));
    Dataset<Row> newDataSet = dataFrameReader.json(newDataRDD.rdd());

    return new ImmutablePair<>(Option.of(newDataSet), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
  }

  private JavaRDD<String> toRDD(OffsetRange[] offsetRanges) {
    return KafkaUtils.createRDD(sparkContext, offsetGen.getKafkaParams(), offsetRanges,
        LocationStrategies.PreferConsistent()).map(x -> (String) x.value());
  }

  @Override
  public void onCommit(String lastCkptStr) {
    if (this.props.getBoolean(KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.key(), KafkaOffsetGen.Config.ENABLE_KAFKA_COMMIT_OFFSET.defaultValue())) {
      offsetGen.commitOffsetToKafka(lastCkptStr);
    }
  }
}

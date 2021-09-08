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

package org.apache.hudi.utilities.transform;

import org.apache.hudi.common.config.TypedProperties;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DebeziumTransformer implements Transformer {

  private static final Logger LOG = LogManager.getLogger(DebeziumTransformer.class);

  @Override
  public Dataset apply(JavaSparkContext jsc, SparkSession sparkSession, Dataset<Row> rowDataset,
                       TypedProperties properties) {
    Dataset<Row> transformedDataset = convertColumnToNullable(sparkSession, rowDataset, Arrays.asList(rowDataset.columns()));
    transformedDataset = convertSparkArrayColumnsToString(transformedDataset);

    LOG.info(String.format("Transformed Kafka Payload Schema:\n%s", transformedDataset.schema().treeString()));

    return transformedDataset;
  }

  /**
   * Utility function for converting columns to nullable. This is useful when required to make a column nullable to match a nullable column from Debezium change
   * events.
   *
   * @param sparkSession SparkSession object
   * @param dataset      Dataframe to modify
   * @param columns      Columns to convert to nullable
   * @return Modified dataframe
   */
  private static Dataset<Row> convertColumnToNullable(SparkSession sparkSession, Dataset<Row> dataset, List<String> columns) {
    StructField[] modifiedStructFields = Arrays.stream(dataset.schema().fields()).map(field -> columns
        .contains(field.name()) ? new StructField(field.name(), field.dataType(), true, field.metadata()) : field)
        .toArray(StructField[]::new);

    return sparkSession.createDataFrame(dataset.rdd(), new StructType(modifiedStructFields));
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

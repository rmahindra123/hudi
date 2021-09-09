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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.utilities.schema.SchemaProvider;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * This should only be used as a one-time snapshot of a database table. Afterwards, the table should be updated via
 * Debezium streaming source.
 *
 * @see DebeziumSource
 */
public class DebeziumBootstrapSource extends JdbcSource {

  private static final Logger LOG = LogManager.getLogger(DebeziumBootstrapSource.class);

  public DebeziumBootstrapSource(TypedProperties props,
                                 JavaSparkContext sparkContext,
                                 SparkSession sparkSession,
                                 SchemaProvider schemaProvider) throws Exception {
    super(props, sparkContext, sparkSession, schemaProvider);
  }

  @Override
  protected Pair<Option<Dataset<Row>>, String> fetchNextBatch(Option<String> lastCkptStr, long sourceLimit) {
    if (!lastCkptStr.isPresent() || lastCkptStr.get() == null) {
      throw new HoodieException("There must be a previous checkpoint recording current Kafka offsets");
    }

    LOG.info("Current Kafka offset recorded as : " + lastCkptStr.get());

    return super.fetchNextBatch(lastCkptStr, sourceLimit);
  }
}

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

package org.apache.hudi.utilities.sources.helpers;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DebeziumConstants {

  // INPUT COLUMNS
  public static final String INP_OP_FIELD = "op";
  public static final String INP_AFTER_FIELD = "after";
  public static final String INP_BEFORE_FIELD = "before";
  public static final String INP_TS_MS_FIELD = "ts_ms";
  public static final String INP_SOURCE_FIELD = "source";
  public static final String INP_SOURCE_NAME_FIELD = "name";
  public static final String INP_SOURCE_TS_MS_FIELD = "ts_ms";
  public static final String INP_SOURCE_TXID_FIELD = "txId";
  public static final String INP_SOURCE_LSN_FIELD = "lsn";
  public static final String INP_SOURCE_XMIN_FIELD = "xmin";

  public static final String INP_QUALIFIED_SOURCE_NAME_FIELD = "source.name";
  public static final String INP_QUALIFIED_SOURCE_TS_MS_FIELD = "source.ts_ms";
  public static final String INP_QUALIFIED_SOURCE_TXID_FIELD = "source.txId";
  public static final String INP_QUALIFIED_SOURCE_LSN_FIELD = "source.lsn";
  public static final String INP_QUALIFIED_SOURCE_XMIN_FIELD = "source.xmin";

  // OUTPUT COLUMNS
  public static final String MODIFIED_OP_COL_NAME = "_change_operation_type";
  public static final String SOURCE_TS_COL_NAME = "_event_origin_ts_ms";
  public static final String SOURCE_TX_ID_COL_NAME = "_event_tx_id";
  public static final String SOURCE_LSN_COL_NAME = "_event_lsn";
  public static final String SOURCE_XMIN_COL_NAME = "_event_xmin";
  public static final String UPSTREAM_PROCESSING_TS_COL_NAME = "_upstream_event_processed_ts_ms";
  public static final String MODIFIED_SHARD_NAME = "db_shard_source_partition";

  // Other Constants
  public static final String DELETE_OP = "d";
  public static final String IS_INITIAL_CHECKPOINT_CONFIG_KEY = "hoodie.debezium.initial.checkpoint";

  // List of meta data columns
  public static List<String> META_COLUMNS = Collections.unmodifiableList(Arrays.asList(
      MODIFIED_OP_COL_NAME,
      UPSTREAM_PROCESSING_TS_COL_NAME,
      SOURCE_TS_COL_NAME,
      SOURCE_TX_ID_COL_NAME,
      SOURCE_LSN_COL_NAME,
      SOURCE_XMIN_COL_NAME,
      MODIFIED_SHARD_NAME
  ));

  // List of unsupported columns by Debezium
  public static List<String> UNSUPPORTED_COLUMN_TYPES = Collections.unmodifiableList(Arrays.asList(
      "box",
      "circle",
      "line",
      "lseg",
      "path",
      "pg_lsn",
      "pg_snapshot",
      "polygon",
      "tsquery",
      "tsvector",
      "txid_snapshot"
  ));
}

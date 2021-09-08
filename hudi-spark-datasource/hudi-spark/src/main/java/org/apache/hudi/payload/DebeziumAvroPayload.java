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

package org.apache.hudi.payload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

/**
 * Provides support for seamlessly applying changes captured via Debezium.
 * <p>
 * Debezium change event types are determined for the op field in the payload
 * <p>
 * - For inserts, op=i
 * - For deletes, op=d
 * - For updates, op=u
 * - For snapshort inserts, op=r
 * <p>
 * This payload implementation will issue matching insert, delete, updates against the hudi table
 */
public class DebeziumAvroPayload extends OverwriteWithLatestAvroPayload {

  private static final String SOURCE_TS_COL_NAME = "_event_origin_ts_ms";
  private static final String UPSTREAM_PROCESSING_TS_COL_NAME = "_upstream_event_processed_ts_ms";
  private static final String SOURCE_LSN_COL_NAME = "_event_lsn";
  private static final String MODIFIED_OP_COL_NAME = "_change_operation_type";
  private static final String DELETE_OP = "d";

  private final Map<String, String> timestampMap = new HashMap<>();
  public static final String DEBEZIUM_TOASTED_VALUE = "__debezium_unavailable_value";

  public DebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
    Object sourceTsObj = record.get(SOURCE_TS_COL_NAME);
    Object debeziumTsObj = record.get(UPSTREAM_PROCESSING_TS_COL_NAME);
    if (null != sourceTsObj) {
      timestampMap.put(SOURCE_TS_COL_NAME, sourceTsObj.toString());
    }
    if (null != debeziumTsObj) {
      timestampMap.put(UPSTREAM_PROCESSING_TS_COL_NAME, debeziumTsObj.toString());
    }
  }

  public DebeziumAvroPayload(Option<GenericRecord> record) {
    this(record.get(), (record1) -> 0); // natural order
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    // Guaranteed to be generic record here.
    GenericRecord currRec = (GenericRecord) currentValue;
    // In the absence of configuration passed to this class, we are hardcoding the fact that
    // source.lsn will be the ordering column passed here.
    Long sourceLSN = (Long) currRec.get(SOURCE_LSN_COL_NAME);
    if (orderingVal.compareTo(sourceLSN) < 0) {
      return Option.of(currentValue);
    }

    Option<IndexedRecord> insertValue = getInsertValue(schema);

    // If the updated record has TOASTED columns, we will need to keep the previous value for those columns
    // see https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-toasted-values
    if (insertValue.isPresent()) {
      List<Schema.Field> fields = insertValue.get().getSchema().getFields();

      fields.forEach(field -> {
        // There are only four avro data types that have unconstrained sizes, which are
        // NON-NULLABLE STRING, NULLABLE STRING, NON-NULLABLE BYTES, NULLABLE BYTES
        boolean isToastedValue =
            (
                (
                    field.schema().getType() == Schema.Type.STRING
                        || (field.schema().getType() == Schema.Type.UNION && field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.STRING))
                )
                    && ((GenericData.Record) insertValue.get()).get(field.name()) != null
                    // Check length first as an optimization
                    && ((CharSequence) ((GenericData.Record) insertValue.get()).get(field.name())).length() == DEBEZIUM_TOASTED_VALUE.length()
                    && DEBEZIUM_TOASTED_VALUE.equals(((CharSequence) ((GenericData.Record) insertValue.get()).get(field.name())).toString())
            )
                ||
                (
                    (
                        field.schema().getType() == Schema.Type.BYTES
                            || (field.schema().getType() == Schema.Type.UNION && field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.BYTES))
                    )
                        && ((GenericData.Record) insertValue.get()).get(field.name()) != null
                        && ((ByteBuffer) ((GenericData.Record) insertValue.get()).get(field.name())).array().length == DEBEZIUM_TOASTED_VALUE.length()
                        && DEBEZIUM_TOASTED_VALUE.equals(new String(((ByteBuffer) ((GenericData.Record) insertValue.get()).get(field.name())).array(), StandardCharsets.UTF_8))
                );

        if (isToastedValue) {
          ((GenericData.Record) insertValue.get()).put(field.name(), ((GenericData.Record) currentValue).get(field.name()));
        }
      });
    }

    return insertValue;
  }

  @Override
  protected boolean isDeleteRecord(GenericRecord genericRecord) {
    Object value = genericRecord.get(MODIFIED_OP_COL_NAME);
    return value != null && value.toString().equalsIgnoreCase(DELETE_OP);
  }

  public Option<Map<String, String>> getMetadata() {
    return Option.of(timestampMap);
  }
}

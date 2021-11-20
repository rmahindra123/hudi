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

package org.apache.hudi.common.model.debezium;

import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Provides support for seamlessly applying changes captured via Debezium for PostgresDB.
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
public class PostgresDebeziumAvroPayload extends OverwriteWithLatestAvroPayload {

  private static final Logger LOG = LogManager.getLogger(PostgresDebeziumAvroPayload.class);
  public static final String DEBEZIUM_TOASTED_VALUE = "__debezium_unavailable_value";

  public PostgresDebeziumAvroPayload(GenericRecord record, Comparable orderingVal) {
    super(record, orderingVal);
  }

  public PostgresDebeziumAvroPayload(Option<GenericRecord> record) {
    super(record);
  }

  private Option<IndexedRecord> handleDeleteOperation(IndexedRecord insertRecord) {
    boolean delete = false;
    if (insertRecord instanceof GenericRecord) {
      GenericRecord record = (GenericRecord) insertRecord;
      Object value = record.get(DebeziumConstants.MODIFIED_OP_COL_NAME);
      delete = value != null && value.toString().equalsIgnoreCase(DebeziumConstants.DELETE_OP);
    }

    return delete ? Option.empty() : Option.of(insertRecord);
  }

  private Long extractLSN(IndexedRecord record) {
    GenericRecord genericRecord = (GenericRecord) record;
    return (Long) genericRecord.get(DebeziumConstants.MODIFIED_LSN_COL_NAME);
  }

  private boolean shouldPickCurrentRecord(IndexedRecord currentRecord, Schema schema) throws IOException {
    IndexedRecord insertRecord = super.getInsertValue(schema).get();
    Long currentSourceLSN = extractLSN(currentRecord);
    Long insertSourceLSN = extractLSN(insertRecord);
    // Pick the current value in storage only if its LSN is latest compared to the LSN of the insert value
    return insertSourceLSN < currentSourceLSN;
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    IndexedRecord insertRecord = super.getInsertValue(schema).get();
    return handleDeleteOperation(insertRecord);
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema) throws IOException {
    // Step 1: If the LSN of the current record in storage is higher than the LSN of the
    // insert record (including a delete record), pick the current record.
    if (shouldPickCurrentRecord(currentValue, schema)) {
      return Option.of(currentValue);
    }

    // Step 2: Pick the insert record (as a delete record),
    // but get toasted column values from current record
    Option<IndexedRecord> insertOrDeleteRecord = getInsertValue(schema);

    if (insertOrDeleteRecord.isPresent()) {
      List<Schema.Field> fields = insertOrDeleteRecord.get().getSchema().getFields();

      // If the updated record has TOASTED columns, we will need to keep the previous value for those columns
      // see https://debezium.io/documentation/reference/connectors/postgresql.html#postgresql-toasted-values
      fields.forEach(field -> {
        // There are only four avro data types that have unconstrained sizes, which are
        // NON-NULLABLE STRING, NULLABLE STRING, NON-NULLABLE BYTES, NULLABLE BYTES
        boolean isToastedValue =
            (
                (
                    field.schema().getType() == Schema.Type.STRING
                        || (field.schema().getType() == Schema.Type.UNION && field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.STRING))
                )
                    && ((GenericData.Record) insertOrDeleteRecord.get()).get(field.name()) != null
                    // Check length first as an optimization
                    && ((CharSequence) ((GenericData.Record) insertOrDeleteRecord.get()).get(field.name())).length() == DEBEZIUM_TOASTED_VALUE.length()
                    && DEBEZIUM_TOASTED_VALUE.equals(((CharSequence) ((GenericData.Record) insertOrDeleteRecord.get()).get(field.name())).toString())
            )
                ||
                (
                    (
                        field.schema().getType() == Schema.Type.BYTES
                            || (field.schema().getType() == Schema.Type.UNION && field.schema().getTypes().stream().anyMatch(s -> s.getType() == Schema.Type.BYTES))
                    )
                        && ((GenericData.Record) insertOrDeleteRecord.get()).get(field.name()) != null
                        && ((ByteBuffer) ((GenericData.Record) insertOrDeleteRecord.get()).get(field.name())).array().length == DEBEZIUM_TOASTED_VALUE.length()
                        && DEBEZIUM_TOASTED_VALUE.equals(new String(((ByteBuffer) ((GenericData.Record) insertOrDeleteRecord.get()).get(field.name())).array(), StandardCharsets.UTF_8))
                );

        if (isToastedValue) {
          ((GenericData.Record) insertOrDeleteRecord.get()).put(field.name(), ((GenericData.Record) currentValue).get(field.name()));
        }
      });
    }
    return insertOrDeleteRecord;
  }
}


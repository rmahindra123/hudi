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

package org.apache.hudi.common.util.collection;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.testutils.HoodieCommonTestHarness;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.testutils.SpillableMapTestUtils;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer.Alphanumeric;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests external spillable map {@link ExternalSpillableMap}.
 */
@TestMethodOrder(Alphanumeric.class)
public class TestExternalSpillableMap extends HoodieCommonTestHarness {

  private static String failureOutputPath;

  @BeforeEach
  public void setUp() {
    initPath();
    failureOutputPath = basePath + "/test_fail";
  }

  @ParameterizedTest
  @ValueSource(strings = {"disk_map", "rock_db"})
  public void simpleInsertTest(String diskMapType) throws IOException, URISyntaxException {
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    String payloadClazz = HoodieAvroPayload.class.getName();
    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), ExternalSpillableMap.DiskMapType.fromValue(diskMapType)); // 16B
    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    List<HoodieRecord> oRecords = new ArrayList<>();
    while (itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      oRecords.add(rec);
      assert recordKeys.contains(rec.getRecordKey());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"disk_map", "rock_db"})
  public void testSimpleUpsert(String diskMapType) throws IOException, URISyntaxException {
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), ExternalSpillableMap.DiskMapType.fromValue(diskMapType));// 16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    while (itr.hasNext()) {
      HoodieRecord<? extends HoodieRecordPayload> rec = itr.next();
      assert recordKeys.contains(rec.getRecordKey());
    }
    List<IndexedRecord> updatedRecords = SchemaTestUtil.updateHoodieTestRecords(recordKeys,
        SchemaTestUtil.generateHoodieTestRecords(0, 100), HoodieActiveTimeline.createNewInstantTime());

    // update records already inserted
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);

    // make sure we have records spilled to disk
    assertTrue(records.getDiskBasedMapNumEntries() > 0);

    // iterate over the updated records and compare the value from Map
    updatedRecords.forEach(record -> {
      HoodieRecord rec = records.get(((GenericRecord) record).get(HoodieRecord.RECORD_KEY_METADATA_FIELD));
      try {
        assertEquals(rec.getData().getInsertValue(schema).get(), record);
      } catch (IOException io) {
        throw new UncheckedIOException(io);
      }
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"disk_map", "rock_db"})
  public void testAllMapOperations(String diskMapType) throws IOException, URISyntaxException {

    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());
    String payloadClazz = HoodieAvroPayload.class.getName();

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), ExternalSpillableMap.DiskMapType.fromValue(diskMapType));

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    // insert a bunch of records so that values spill to disk too
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    IndexedRecord inMemoryRecord = iRecords.get(0);
    String ikey = ((GenericRecord) inMemoryRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String iPartitionPath = ((GenericRecord) inMemoryRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieRecord inMemoryHoodieRecord = new HoodieRecord<>(new HoodieKey(ikey, iPartitionPath),
        new HoodieAvroPayload(Option.of((GenericRecord) inMemoryRecord)));

    IndexedRecord onDiskRecord = iRecords.get(99);
    String dkey = ((GenericRecord) onDiskRecord).get(HoodieRecord.RECORD_KEY_METADATA_FIELD).toString();
    String dPartitionPath = ((GenericRecord) onDiskRecord).get(HoodieRecord.PARTITION_PATH_METADATA_FIELD).toString();
    HoodieRecord onDiskHoodieRecord = new HoodieRecord<>(new HoodieKey(dkey, dPartitionPath),
        new HoodieAvroPayload(Option.of((GenericRecord) onDiskRecord)));
    // assert size
    assert records.size() == 100;
    // get should return the same HoodieKey, same location and same value
    assert inMemoryHoodieRecord.getKey().equals(records.get(ikey).getKey());
    assert onDiskHoodieRecord.getKey().equals(records.get(dkey).getKey());
    // compare the member variables of HoodieRecord not set by the constructor
    assert records.get(ikey).getCurrentLocation().getFileId().equals(SpillableMapTestUtils.DUMMY_FILE_ID);
    assert records.get(ikey).getCurrentLocation().getInstantTime().equals(SpillableMapTestUtils.DUMMY_COMMIT_TIME);

    // test contains
    assertTrue(records.containsKey(ikey));
    assertTrue(records.containsKey(dkey));

    // test isEmpty
    assertFalse(records.isEmpty());

    // test containsAll
    assertTrue(records.keySet().containsAll(recordKeys));

    // remove (from inMemory and onDisk)
    HoodieRecord removedRecord = records.remove(ikey);
    assertTrue(removedRecord != null);
    assertFalse(records.containsKey(ikey));

    removedRecord = records.remove(dkey);
    assertTrue(removedRecord != null);
    assertFalse(records.containsKey(dkey));

    // test clear
    records.clear();
    assertTrue(records.size() == 0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"disk_map", "rock_db"})
  public void simpleTestWithException(String diskMapType) throws IOException, URISyntaxException {
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records = new ExternalSpillableMap<>(16L,
        failureOutputPath, new DefaultSizeEstimator(),
        new HoodieRecordSizeEstimator(schema), ExternalSpillableMap.DiskMapType.fromValue(diskMapType)); // 16B

    List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
    List<String> recordKeys = SpillableMapTestUtils.upsertRecords(iRecords, records);
    assert (recordKeys.size() == 100);
    Iterator<HoodieRecord<? extends HoodieRecordPayload>> itr = records.iterator();
    assertThrows(IOException.class, () -> {
      while (itr.hasNext()) {
        throw new IOException("Testing failures...");
      }
    });
  }

  @ParameterizedTest
  @ValueSource(strings = {"disk_map", "rock_db"})
  public void testDataCorrectnessWithUpsertsToDataInMapAndOnDisk(String diskMapType) throws IOException, URISyntaxException {

    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), ExternalSpillableMap.DiskMapType.fromValue(diskMapType)); // 16B

    List<String> recordKeys = new ArrayList<>();
    // Ensure we spill to disk
    while (records.getDiskBasedMapNumEntries() < 1) {
      List<IndexedRecord> iRecords = SchemaTestUtil.generateHoodieTestRecords(0, 100);
      recordKeys.addAll(SpillableMapTestUtils.upsertRecords(iRecords, records));
    }

    // Get a record from the in-Memory map
    String key = recordKeys.get(0);
    HoodieRecord record = records.get(key);
    List<IndexedRecord> recordsToUpdate = new ArrayList<>();
    recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema).get());

    String newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    List<String> keysToBeUpdated = new ArrayList<>();
    keysToBeUpdated.add(key);
    // Update the instantTime for this record
    List<IndexedRecord> updatedRecords =
        SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
    // Upsert this updated record
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);
    GenericRecord gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated commitTime
    assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());

    // Get a record from the disk based map
    key = recordKeys.get(recordKeys.size() - 1);
    record = records.get(key);
    recordsToUpdate = new ArrayList<>();
    recordsToUpdate.add((IndexedRecord) record.getData().getInsertValue(schema).get());

    newCommitTime = HoodieActiveTimeline.createNewInstantTime();
    keysToBeUpdated = new ArrayList<>();
    keysToBeUpdated.add(key);
    // Update the commitTime for this record
    updatedRecords = SchemaTestUtil.updateHoodieTestRecords(keysToBeUpdated, recordsToUpdate, newCommitTime);
    // Upsert this updated record
    SpillableMapTestUtils.upsertRecords(updatedRecords, records);
    gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated instantTime
    assert newCommitTime.contentEquals(gRecord.get(HoodieRecord.COMMIT_TIME_METADATA_FIELD).toString());
  }

  @ParameterizedTest
  @ValueSource(strings = {"disk_map", "rock_db"})
  public void testDataCorrectnessWithoutHoodieMetadata(String diskMapType) throws IOException, URISyntaxException {

    Schema schema = SchemaTestUtil.getSimpleSchema();

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> records =
        new ExternalSpillableMap<>(16L, basePath, new DefaultSizeEstimator(),
            new HoodieRecordSizeEstimator(schema), ExternalSpillableMap.DiskMapType.fromValue(diskMapType));

    List<String> recordKeys = new ArrayList<>();
    // Ensure we spill to disk
    while (records.getDiskBasedMapNumEntries() < 1) {
      List<HoodieRecord> hoodieRecords = SchemaTestUtil.generateHoodieTestRecordsWithoutHoodieMetadata(0, 100);
      hoodieRecords.stream().forEach(r -> {
        records.put(r.getRecordKey(), r);
        recordKeys.add(r.getRecordKey());
      });
    }

    // Get a record from the in-Memory map
    String key = recordKeys.get(0);
    HoodieRecord record = records.get(key);
    // Get the field we want to update
    String fieldName = schema.getFields().stream().filter(field -> field.schema().getType() == Schema.Type.STRING)
        .findAny().get().name();
    // Use a new value to update this field
    String newValue = "update1";
    List<HoodieRecord> recordsToUpdate = new ArrayList<>();
    recordsToUpdate.add(record);

    List<HoodieRecord> updatedRecords =
        SchemaTestUtil.updateHoodieTestRecordsWithoutHoodieMetadata(recordsToUpdate, schema, fieldName, newValue);

    // Upsert this updated record
    updatedRecords.forEach(r -> {
      records.put(r.getRecordKey(), r);
    });
    GenericRecord gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated value for the field name
    assertEquals(gRecord.get(fieldName).toString(), newValue);

    // Get a record from the disk based map
    key = recordKeys.get(recordKeys.size() - 1);
    record = records.get(key);
    // Get the field we want to update
    fieldName = schema.getFields().stream().filter(field -> field.schema().getType() == Schema.Type.STRING).findAny()
        .get().name();
    // Use a new value to update this field
    newValue = "update2";
    recordsToUpdate = new ArrayList<>();
    recordsToUpdate.add(record);

    updatedRecords =
        SchemaTestUtil.updateHoodieTestRecordsWithoutHoodieMetadata(recordsToUpdate, schema, fieldName, newValue);

    // Upsert this updated record
    updatedRecords.forEach(r -> {
      records.put(r.getRecordKey(), r);
    });
    gRecord = (GenericRecord) records.get(key).getData().getInsertValue(schema).get();
    // The record returned for this key should have the updated value for the field name
    assertEquals(gRecord.get(fieldName).toString(), newValue);
  }

  // TODO : come up with a performance eval test for spillableMap
  @Test
  public void testLargeInsertUpsert() throws IOException, URISyntaxException {

    final int numRecordsInFile = 3200;
    final int numLoopFile = 1500; // 1500 for rockDB and 500 for diskBasedMap
    final int numTotalRecords = numRecordsInFile * numLoopFile;
    Schema schema = HoodieAvroUtils.addMetadataFields(SchemaTestUtil.getSimpleSchema());

    ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> externalSpillableMap =
            new ExternalSpillableMap<>(500000000L, basePath,
                    new DefaultSizeEstimator(), new HoodieRecordSizeEstimator(schema),
                    ExternalSpillableMap.DiskMapType.COMPRESSED_DISK_MAP); // 16B

    List<String> overallKeys = new ArrayList<>();
    List<IndexedRecord> fixedRecordPayload = SchemaTestUtil.generateTestRecords(0, numRecordsInFile);
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    List<IndexedRecord> subRecords;
    for (int inputCnt = 0; inputCnt < numLoopFile; inputCnt++) {
      // insert a bunch of externalSpillableMap so that values spill to disk too
      if (inputCnt%50 == 0) {
        instantTime = HoodieActiveTimeline.createNewInstantTime();
      }
      subRecords = SchemaTestUtil.generateHoodieTestRecords(fixedRecordPayload, instantTime);
      long startTimeMs = System.currentTimeMillis();
      overallKeys.addAll(SpillableMapTestUtils.upsertRecords(subRecords, externalSpillableMap));
      //System.out.println("SIZES OF subRecords and externalSpillableMap (spillable map)" + subRecords.size() + " " + externalSpillableMap.size() + " " + basePath + " " + (System.currentTimeMillis() - startTimeMs));
      System.out.println("SIZES OF subRecords " + subRecords.size() + " " + basePath + " " + (System.currentTimeMillis() - startTimeMs));
      if (inputCnt%50==0) {
        System.out.println("MAP STATS " + externalSpillableMap.getDiskBasedMapNumEntries() + " " + externalSpillableMap.getSizeOfFileOnDiskInBytes() + " " + externalSpillableMap.getInMemoryMapNumEntries() + " " + externalSpillableMap.getCurrentInMemoryMapSize());
      }
    }

    new StressTestSpillableMap(
            1,
            10000,//250000,
            overallKeys,
            numTotalRecords,
            externalSpillableMap).start();

    /*new StressTestSpillableMap(
            5,
            500001,
            overallKeys,
            numTotalRecords,
            externalSpillableMap).start();*/

    System.out.println("MAP STATS " + externalSpillableMap.getDiskBasedMapNumEntries() + " " + externalSpillableMap.getSizeOfFileOnDiskInBytes() + " " + externalSpillableMap.getInMemoryMapNumEntries());
  }

  public static class StressTestSpillableMap {

    private final int numThreads;
    private final int maxStressCnt;
    private final List<String > overallKeys;
    private final int numTotalRecords;
    private final ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> externalSpillableMap;
    private final CountDownLatch latch;

    public StressTestSpillableMap(int numThreads, int maxStressCnt, List<String> overallKeys, int numTotalRecords, ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> externalSpillableMap) {
      this.numThreads = numThreads;
      this.maxStressCnt = maxStressCnt;
      this.overallKeys = overallKeys;
      this.numTotalRecords = numTotalRecords;
      this.externalSpillableMap = externalSpillableMap;
      this.latch = new CountDownLatch(numThreads);
    }

    public void start() {
      System.out.println("Starting one run with numThreads: " + numThreads + " and stressCnt " + maxStressCnt);
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      long startTimeMs = System.currentTimeMillis();
      for (int i = 0; i < numThreads; i++) {
        executorService.execute(new GetPutRequestGenerator(i+1,maxStressCnt/numThreads, overallKeys, numTotalRecords, externalSpillableMap, latch));
      }
      try {
        latch.await();
      } catch (Exception exc) {
        System.out.println("OH NO " + exc.getMessage());
      }
      long endTimeMs = System.currentTimeMillis();
      System.out.println("ALL THREADS DONE TOTAL TIME " + (endTimeMs - startTimeMs));

    }

    public static class GetPutRequestGenerator implements Runnable {

      private static final Random RANDOM = new Random();

      private final int threadIdx;
      private final int maxStressCnt;
      private final List<String > overallKeys;
      private final int numTotalRecords;
      private final ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> externalSpillableMap;
      private final CountDownLatch latch;

      public GetPutRequestGenerator(int threadIdx, int maxStressCnt, List<String> overallKeys, int numTotalRecords, ExternalSpillableMap<String, HoodieRecord<? extends HoodieRecordPayload>> externalSpillableMap, CountDownLatch latch) {
        this.threadIdx = threadIdx;
        this.maxStressCnt = maxStressCnt;
        this.overallKeys = overallKeys;
        this.numTotalRecords = numTotalRecords;
        this.externalSpillableMap = externalSpillableMap;
        this.latch = latch;
      }

      @Override
      public void run() {
        System.out.println("Starting Thread idx: " + threadIdx);
        long stressCnt = 0;

        long getMemoryLatencyAvg = 0;
        int totalGetMemoryCnt = 1;
        long getDiskLatencyAvg = 0;
        int totalGetDiskCnt = 1;

        long putMemoryLatencyAvg = 0;
        int totalPutMemoryCnt = 1;
        long putDiskLatencyAvg = 0;
        int totalPutDiskCnt = 1;

        for (; stressCnt <= maxStressCnt; stressCnt++) {
        /*if (stressCnt % 1000 == 0) {
          System.out.println("THREAD IDX " + threadIdx + " ENTER STRESS CNT " + stressCnt + " AT " + System.currentTimeMillis());
        }*/
          // First do a random GET
          //Do not benchmark this
          String actualKey = overallKeys.get(RANDOM.nextInt(numTotalRecords));

          // Start benchmark
          long startTimeMs = System.nanoTime();
          HoodieRecord lastReadRecord = externalSpillableMap.get(actualKey);
          long completeTimeMs = System.nanoTime();
          Long totalLatency = (completeTimeMs - startTimeMs);
          if (totalLatency >= 0) {
            String type = "UNKNOWN";
            if (externalSpillableMap.inMemoryContainsKey(actualKey)) {
              type = "MEMORY";
              getMemoryLatencyAvg += totalLatency;
              totalGetMemoryCnt += 1;
            } else if (externalSpillableMap.inDiskContainsKey(actualKey)) {
              type = "DISK";
              getDiskLatencyAvg += totalLatency;
              totalGetDiskCnt += 1;
            } else {
              System.out.println("GET Key is neither in disk nor mem " + actualKey);
            }
            //System.out.println("GET " + type + " TIME " + (totalLatency) + " KEY " + actualKey);
          }

          // Second do a random PUT, ensure we pick another random key
          actualKey = overallKeys.get(RANDOM.nextInt(numTotalRecords));

          startTimeMs = System.nanoTime();
          externalSpillableMap.put(actualKey, lastReadRecord);
          completeTimeMs = System.nanoTime();
          totalLatency = (completeTimeMs - startTimeMs);
          if (totalLatency >= 0) {
            String type = "UNKNOWN";
            if (externalSpillableMap.inMemoryContainsKey(actualKey)) {
              type = "MEMORY";
              putMemoryLatencyAvg += totalLatency;
              totalPutMemoryCnt += 1;
            } else if (externalSpillableMap.inDiskContainsKey(actualKey)) {
              type = "DISK";
              putDiskLatencyAvg += totalLatency;
              totalPutDiskCnt += 1;
            } /*else {
            System.out.println("PUT Key is neither in disk nor mem " + actualKey);
          }*/
            //System.out.println("PUT " + type + " TIME " + (totalLatency) + " KEY " + actualKey);
          }
        }

        System.out.println("THREAD IDX " + threadIdx + "LATENCY STATS NUM RECORDS " + stressCnt + " GET MEM " + getMemoryLatencyAvg/totalGetMemoryCnt + " GET DISK " + getDiskLatencyAvg/totalGetDiskCnt + " PUT MEM " + putMemoryLatencyAvg/totalPutMemoryCnt + " PUT DISK " + putDiskLatencyAvg/totalPutDiskCnt);
        latch.countDown();
      }
    }
  }
}

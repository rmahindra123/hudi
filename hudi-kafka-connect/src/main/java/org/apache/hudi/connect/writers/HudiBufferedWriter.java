package org.apache.hudi.connect.writers;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.HoodieRecordSizeEstimator;
import org.apache.hudi.common.util.collection.ExternalSpillableMap;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.IOUtils;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

public class HudiBufferedWriter<T extends HoodieRecordPayload> implements HudiWriter<T> {

  private static final Logger LOG = LoggerFactory.getLogger(HudiBufferedWriter.class);

  private final HoodieEngineContext context;
  private final HoodieJavaWriteClient writeClient;
  private final String instantTime;
  private final HoodieWriteConfig config;
  private ExternalSpillableMap<String, HoodieRecord<T>> bufferedRecords;

  public HudiBufferedWriter(HoodieEngineContext context,
                            HoodieJavaWriteClient writeClient,
                            String instantTime,
                            HoodieWriteConfig config) {
    this.context = context;
    this.writeClient = writeClient;
    this.instantTime = instantTime;
    this.config = config;
    init();
  }

  private void init() {
    try {
      // Load the new records in a map
      long memoryForMerge = IOUtils.getMaxMemoryPerPartitionMerge(context.getTaskContextSupplier(), config);
      LOG.info("MaxMemoryPerPartitionMerge => " + memoryForMerge);
      this.bufferedRecords = new ExternalSpillableMap<>(memoryForMerge,
          config.getSpillableMapBasePath(),
          new DefaultSizeEstimator(),
          new HoodieRecordSizeEstimator(new Schema.Parser().parse(config.getSchema())),
          config.getCommonConfig().getSpillableDiskMapType(),
          config.getCommonConfig().isBitCaskDiskMapCompressionEnabled());
    } catch (IOException io) {
      throw new HoodieIOException("Cannot instantiate an ExternalSpillableMap", io);
    }
  }

  @Override
  public void start() {
  }

  @Override
  public void writeRecord(HoodieRecord<T> record) {
    bufferedRecords.put(record.getRecordKey(), record);
    LOG.info("Number of entries in MemoryBasedMap => "
        + bufferedRecords.getInMemoryMapNumEntries()
        + "Total size in bytes of MemoryBasedMap => "
        + bufferedRecords.getCurrentInMemoryMapSize() + "Number of entries in BitCaskDiskMap => "
        + bufferedRecords.getDiskBasedMapNumEntries() + "Size of file spilled to disk => "
        + bufferedRecords.getSizeOfFileOnDiskInBytes());
  }

  @Override
  public List<WriteStatus> close() {
    try {
      // Write out all records
      List<WriteStatus> writeStatuses = writeClient.insertPreppedRecords(
          bufferedRecords.values().stream().collect(Collectors.toList()),
          instantTime);
      bufferedRecords.close();
      //performMergeDataValidationCheck(writeStatuses);
      return writeStatuses;
    } catch (Exception e) {
      throw new HoodieException("Write records failed", e);
    }
  }
}

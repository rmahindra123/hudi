package org.apache.hudi.connect.writers;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;

import java.util.List;

public interface HudiWriter<T extends HoodieRecordPayload> {

  void start();

  void writeRecord(HoodieRecord<T> record);

  List<WriteStatus> close();
}

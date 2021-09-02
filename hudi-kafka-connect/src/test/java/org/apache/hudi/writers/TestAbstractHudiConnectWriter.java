package org.apache.hudi.writers;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.connect.writers.AbstractHudiConnectWriter;
import org.apache.hudi.connect.writers.HudiConnectConfigs;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.schema.SchemaProvider;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestAbstractHudiConnectWriter {

  private static final String TOPIC_NAME = "kafka-connect-test-topic";
  private static final int PARTITION_NUMBER = 4;
  private final static int NUM_RECORDS = 10;
  private final static int RECORD_KEY_INDEX = 0;

  private HudiConnectConfigs configs;
  private TestKeyGenerator keyGenerator;
  private SchemaProvider schemaProvider;
  private long currentKafkaOffset;

  @BeforeEach
  public void setUp() throws Exception {
    keyGenerator = new TestKeyGenerator(new TypedProperties());
    schemaProvider = new TestSchemaProvider();
  }

  @ParameterizedTest
  @EnumSource(value = TestInputFormats.class)
  public void testAbstractWriterForAllFormats(TestInputFormats inputFormats) throws Exception {
    Schema schema = schemaProvider.getSourceSchema();
    List<?> inputRecords;
    List<HoodieRecord> expectedRecords;

    String formatConverter;
    switch (inputFormats) {
      case JSON_STRING:
        formatConverter = AbstractHudiConnectWriter.KAFKA_STRING_CONVERTER;
        GenericDatumReader<IndexedRecord> reader = new GenericDatumReader<>(schema, schema);
        inputRecords = SchemaTestUtil.generateTestJsonRecords(0, NUM_RECORDS);
        expectedRecords = ((List<String>) inputRecords).stream().map(s -> {
          try {
            return HoodieAvroUtils.rewriteRecord((GenericRecord) reader.read(null, DecoderFactory.get().jsonDecoder(schema, s)),
                schema);
          } catch (IOException exception) {
            throw new HoodieException("Error converting JSON records to AVRO");
          }
        }).map(p -> convertToHoodieRecords(p, p.get(RECORD_KEY_INDEX).toString(), "000/00/00")).collect(Collectors.toList());
        break;
      case AVRO:
        formatConverter = AbstractHudiConnectWriter.KAFKA_AVRO_CONVERTER;
        inputRecords = SchemaTestUtil.generateTestRecords(0, NUM_RECORDS);
        expectedRecords = inputRecords.stream().map(s -> HoodieAvroUtils.rewriteRecord((GenericRecord) s, schema))
            .map(p -> convertToHoodieRecords(p, p.get(RECORD_KEY_INDEX).toString(), "000/00/00")).collect(Collectors.toList());
        break;
      default:
        throw new HoodieException("Unknown test scenario " + inputFormats);
    }

    configs = HudiConnectConfigs.newBuilder()
        .withProperties(
            Collections.singletonMap(HudiConnectConfigs.KAFKA_VALUE_CONVERTER, formatConverter))
        .build();
    AbstractHudiConnectWriterTestWrapper writer = new AbstractHudiConnectWriterTestWrapper(
        configs,
        keyGenerator,
        schemaProvider);

    for (int i = 0; i < NUM_RECORDS; i++) {
      writer.writeRecord(getNextKafkaRecord(inputRecords.get(i)));
    }

    validateRecords(writer.getWrittenRecords(), expectedRecords);
  }
  
  private static void validateRecords(List<HoodieRecord> actualRecords, List<HoodieRecord> expectedRecords) {
    assertEquals(actualRecords.size(), expectedRecords.size());

    actualRecords.sort(Comparator.comparing(HoodieRecord::getRecordKey));
    expectedRecords.sort(Comparator.comparing(HoodieRecord::getRecordKey));

    // iterate through the elements and compare them one by one using
    // the provided comparator.
    Iterator<HoodieRecord> it1 = actualRecords.iterator();
    Iterator<HoodieRecord> it2 = expectedRecords.iterator();
    while (it1.hasNext()) {
      HoodieRecord t1 = it1.next();
      HoodieRecord t2 = it2.next();
      assertEquals(t1.getRecordKey(), t2.getRecordKey());
    }
  }

  private SinkRecord getNextKafkaRecord(Object record) {
    return new SinkRecord(TOPIC_NAME, PARTITION_NUMBER,
        org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA,
        ("key-" + currentKafkaOffset).getBytes(),
        org.apache.kafka.connect.data.Schema.OPTIONAL_BYTES_SCHEMA,
        record, currentKafkaOffset++);
  }

  private static class TestSchemaProvider extends SchemaProvider {

    @Override
    public Schema getSourceSchema() {
      try {
        return SchemaTestUtil.getSimpleSchema();
      } catch (IOException exception) {
        throw new HoodieException("Fatal error parsing schema", exception);
      }
    }
  }

  private static class AbstractHudiConnectWriterTestWrapper extends AbstractHudiConnectWriter {

    private List<HoodieRecord> writtenRecords;

    public AbstractHudiConnectWriterTestWrapper(HudiConnectConfigs connectConfigs, KeyGenerator keyGenerator, SchemaProvider schemaProvider) {
      super(connectConfigs, keyGenerator, schemaProvider);
      writtenRecords = new ArrayList<>();
    }

    public List<HoodieRecord> getWrittenRecords() {
      return writtenRecords;
    }

    @Override
    protected void writeHudiRecord(HoodieRecord<HoodieAvroPayload> record) {
      writtenRecords.add(record);
    }

    @Override
    protected List<WriteStatus> flushHudiRecords() {
      return null;
    }
  }

  private static class TestKeyGenerator extends KeyGenerator {

    protected TestKeyGenerator(TypedProperties config) {
      super(config);
    }

    @Override
    public HoodieKey getKey(GenericRecord record) {
      return new HoodieKey(record.get(RECORD_KEY_INDEX).toString(), "000/00/00");
    }
  }

  private static HoodieRecord convertToHoodieRecords(IndexedRecord iRecord, String key, String partitionPath) {
    return new HoodieRecord<>(new HoodieKey(key, partitionPath),
        new HoodieAvroPayload(Option.of((GenericRecord) iRecord)));
  }

  private enum TestInputFormats {
    AVRO,
    JSON_STRING
  }
}

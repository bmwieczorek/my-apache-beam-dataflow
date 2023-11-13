package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.LogicalTypes;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.util.Arrays;
import java.util.List;

public class ReadParquetFileTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws IOException {
        // given - generate parquet file
        LOGGER.info(INTPUT_SCHEMA.toString(true));
        GenericData.Record record = createGenericRecord(INTPUT_SCHEMA);

        Path path = new Path("target/myNestedRecord.parquet");
        File file = new File(path.toUri().getPath());
        deleteIfPresent(file);

        try (ParquetWriter<GenericData.Record> writer =
             AvroParquetWriter.<GenericData.Record>builder(HadoopOutputFile.fromPath(path, new Configuration()))
                     .withSchema(INTPUT_SCHEMA)
                     .withConf(new Configuration())
                     .withCompressionCodec(CompressionCodecName.SNAPPY)
                     .build()) {

            writer.write(record);
        }

        // when - read parquet file
        PCollection<String> pCollection = pipeline.apply(
            ParquetIO
                .parseGenericRecords(new SerializableFunction<GenericRecord, String>() { // need anonymous type to infer output type
                    @Override
                    public String apply(GenericRecord genericRecord) {
                        Utf8 name = (Utf8) genericRecord.get("myRequiredString");
                        ByteBuffer byteBuffer = (ByteBuffer) genericRecord.get("myRequiredBytes");
                        byte[] bytes = byteBuffer.array();
                        String value = name.toString() + "," + new String(bytes);
                        LOGGER.info("value={}", value);
                        return value;
                    }
                })
                .from("target/myNestedRecord.parquet")
        );

//        pCollection.apply("MyConsoleIO1", MyConsoleIO.write());
//        pCollection.apply("MyConsoleIO2", MyConsoleIO.write());

        // assert
        PAssert.thatSingleton(pCollection).isEqualTo("abc,ABC123");
        pipeline.run().waitUntilFinish();
        deleteIfPresent(file);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(ReadParquetFileTest.class);

    private static final Schema TIMESTAMP_MICROS_LOGICAL_TYPE = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    private static final Schema DATE_LOGICAL_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    private static final Schema DECIMAL_38_LOGICAL_TYPE = LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES));

    private static final Schema INTPUT_SCHEMA =
            SchemaBuilder.record("myRecordName")
                    .fields()
                    .requiredInt("myRequiredInt")
                    .requiredString("myRequiredString")
                    .optionalString("myOptionalString")
                    .nullableString("myNullableString", "myNullableStringDefaultValue")
                    .requiredBoolean("myRequiredBoolean")
                    .requiredBytes("myRequiredBytes")
                    .name("myBytesDecimal").type(DECIMAL_38_LOGICAL_TYPE).noDefault()
                    .name("myRequiredTimestamp").type(TIMESTAMP_MICROS_LOGICAL_TYPE).noDefault() // needs to be timestamp_micros (not timestamp_millis)
                    .name("myOptionalTimestamp").type().optional().type(TIMESTAMP_MICROS_LOGICAL_TYPE)
                    .name("myRequiredDate").doc("Expiration date field").type(DATE_LOGICAL_TYPE).noDefault()
                    .name("myRequiredArrayLongs").type().array().items().longType().noDefault()
                    .name("myRequiredSubRecord").type(SchemaBuilder.record("myRequiredSubRecordType").fields().requiredDouble("myRequiredDouble").requiredBoolean("myRequiredBoolean").endRecord()).noDefault()
                    .name("myOptionalSubRecord").type().optional().record("myOptionalSubRecordType").fields().requiredFloat("myRequiredFloat").requiredBoolean("myRequiredBoolean").endRecord()
                    .name("myNullableSubRecord").type().nullable().record("myNullableSubRecordType").fields().requiredLong("myRequiredLong").requiredBoolean("myRequiredBoolean").endRecord().noDefault()
                    // array of records requires record schema to contain multiple fields
                    .name("myOptionalArraySubRecords").type().optional().array().items(SchemaBuilder.record("myOptionalArraySubRecord")
                            .fields().requiredBoolean("myRequiredBoolean").optionalString("myOptionalBoolean").endRecord())
                    .name("myNullableArraySubRecords").type().nullable().array().items(SchemaBuilder.record("myNullableArraySubRecord")
                            .fields().requiredBoolean("myRequiredString").optionalString("myOptionalString").endRecord()).noDefault()
                    .endRecord();

    private static GenericData.Record createGenericRecord(Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("myRequiredInt", 123);
        record.put("myRequiredString", "abc");
        record.put("myRequiredBoolean", true);
        record.put("myRequiredBytes", ByteBuffer.wrap("ABC123".getBytes()));
        record.put("myBytesDecimal", doubleToByteBuffer(1.23d));
        record.put("myRequiredTimestamp", System.currentTimeMillis() * 1000); // needs to be timestamp_micros (not timestamp_millis)
        record.put("myRequiredDate", (int) new Date(System.currentTimeMillis()).toLocalDate().toEpochDay());
        record.put("myRequiredArrayLongs", Arrays.asList(1L, 2L, 3L));
        Schema myRequiredSubRecordSchema = schema.getField("myRequiredSubRecord").schema();
        GenericRecord myRequiredSubRecord = new GenericData.Record(myRequiredSubRecordSchema);
        myRequiredSubRecord.put("myRequiredDouble", 1.0d);
        myRequiredSubRecord.put("myRequiredBoolean", false);
        record.put("myRequiredSubRecord", myRequiredSubRecord);
        Schema myOptionalSubRecordSchema = unwrapSchema(schema.getField("myOptionalSubRecord").schema());
        GenericRecord myOptionalSubRecord = new GenericData.Record(myOptionalSubRecordSchema);
        myOptionalSubRecord.put("myRequiredFloat", 2.0f);
        myOptionalSubRecord.put("myRequiredBoolean", true);
        record.put("myOptionalSubRecord", myOptionalSubRecord);
        Schema myNullableSubRecordSchema = unwrapSchema(schema.getField("myNullableSubRecord").schema());
        GenericRecord myNullableSubRecord = new GenericData.Record(myNullableSubRecordSchema);
        myNullableSubRecord.put("myRequiredLong", 12L);
        myNullableSubRecord.put("myRequiredBoolean", false);
        record.put("myNullableSubRecord", myNullableSubRecord);
        Schema myOptionalArraySubRecords = schema.getField("myOptionalArraySubRecords").schema();
        Schema arraySubRecordSchema = unwrapSchema(myOptionalArraySubRecords).getElementType();
        GenericRecord mySubRecord1 = new GenericData.Record(arraySubRecordSchema);
        mySubRecord1.put("myRequiredBoolean", true);
        GenericRecord mySubRecord2 = new GenericData.Record(arraySubRecordSchema);
        mySubRecord2.put("myRequiredBoolean", false);
        record.put("myOptionalArraySubRecords", Arrays.asList(mySubRecord1, mySubRecord2));
        return record;
    }

    private static Schema unwrapSchema(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            List<Schema> types = schema.getTypes();
            return types.get(0).getType() == Schema.Type.NULL ? types.get(1) : types.get(0);
        }
        return schema;
    }

    private static ByteBuffer doubleToByteBuffer(double d) {
        BigDecimal bigDecimal = BigDecimal.valueOf(d).setScale(9, RoundingMode.UNNECESSARY);
        BigInteger bigInteger = bigDecimal.unscaledValue();
        return ByteBuffer.wrap(bigInteger.toByteArray());
    }

    private static void deleteIfPresent(File file) {
        if (file.exists()) {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }
}

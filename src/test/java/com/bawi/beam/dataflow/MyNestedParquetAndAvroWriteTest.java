package com.bawi.beam.dataflow;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.LocalOutputFile;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Paths;
import java.sql.Date;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;


public class MyNestedParquetAndAvroWriteTest implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyNestedParquetAndAvroWriteTest.class);

    @Test
    public void test() throws IOException, URISyntaxException {
        // for Windows only - avoid warning: org.apache.hadoop.util.Shell:746 - Did not find winutils.exe, java.io.FileNotFoundException: HADOOP_HOME and hadoop.home.dir are unset
        System.setProperty("hadoop.home.dir", Paths.get("src/test/resources/hadoop-3.0.0").toFile().getAbsolutePath());

        // given - generate parquet file
        LOGGER.info(INTPUT_SCHEMA.toString(true));

        generateParquetFile("target/myEmptyNestedRecord.parquet", null);
        generateParquetFile("target/myNestedRecord.parquet", createGenericRecord(INTPUT_SCHEMA));

        generateAvroFile("target/myEmptyNestedRecord.snappy.avro", null);
        generateAvroFile("target/myNestedRecord.snappy.avro", createGenericRecord(INTPUT_SCHEMA));

    }

    private static void generateAvroFile(String avroEmptyFileRelativePath, GenericData.Record record) throws IOException {
        Path avroPath = new Path(avroEmptyFileRelativePath);
        File avroFile = new File(avroPath.toUri().getPath());
        deleteIfPresent(avroFile);

        try (DataFileWriter<GenericData.Record> writer = new DataFileWriter<>(new GenericDatumWriter<>(INTPUT_SCHEMA))) {
            writer.setCodec(CodecFactory.snappyCodec());
            writer.create(INTPUT_SCHEMA, avroFile);
            if (record != null) {
                writer.append(record);
            }
        }
    }

    private static void generateParquetFile(String fileRelativePath, GenericData.Record record) throws URISyntaxException, IOException {
        Path path = new Path(fileRelativePath);
        File file = new File(path.toUri().getPath());
        deleteIfPresent(file);

        FileSystem fs = FileSystems.getFileSystem(new URI("file", null, "/", null, null));
        java.nio.file.Path nioOutputPath = fs.getPath(fileRelativePath);
        LocalOutputFile outputFile = new LocalOutputFile(nioOutputPath);

        Configuration conf = new Configuration();
        conf.setBoolean("parquet.avro.write-old-list-structure", false); // used for record.put("myRequiredArrayNullableInts", Arrays.asList(1, null, 3));
        try (ParquetWriter<GenericData.Record> writer =
                     AvroParquetWriter.<GenericData.Record>builder(outputFile)
                     .withSchema(INTPUT_SCHEMA)
                     .withConf(conf)
                     .withCompressionCodec(CompressionCodecName.SNAPPY)
                     .build()) {

            if (record != null) {
                writer.write(record);
            }
        }
    }


    private static final Schema TIMESTAMP_MICROS_LOGICAL_TYPE = LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
    private static final Schema DATE_LOGICAL_TYPE = LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT));
    private static final Schema TIME_LOGICAL_TYPE = LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG));
    private static final Schema DATETIME_LOGICAL_TYPE = LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG));
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
                    .name("myRequiredTime").doc("My required time field").type(TIME_LOGICAL_TYPE).noDefault()
                    .name("myRequiredDateTime").doc("My required datetime field").type(DATETIME_LOGICAL_TYPE).noDefault()
                    .name("myRequiredArrayLongs").type().array().items().longType().noDefault()
//                    .name("myRequiredArrayNullableInts").type().array().items().nullable().longType().noDefault() // for avro generation: org.apache.avro.file.DataFileWriter$AppendWriteException: org.apache.avro.UnresolvedUnionException: Not in union {"type":"array","items":["long","null"]}: [1, 3] (field=myRequiredArrayNullableInts)
                    .name("myRequiredArrayNullableInts").type().optional().array().items().longType()
                    .name("myRequiredSubRecord").type(SchemaBuilder.record("myRequiredSubRecordType").fields().requiredDouble("myRequiredDouble").requiredBoolean("myRequiredBoolean").endRecord()).noDefault()
                    .name("myOptionalSubRecord").type().optional().record("myOptionalSubRecordType").fields().requiredFloat("myRequiredFloat").requiredBoolean("myRequiredBoolean").endRecord()
                    .name("myNullableSubRecord").type().nullable().record("myNullableSubRecordType").fields().requiredLong("myRequiredLong").requiredBoolean("myRequiredBoolean").endRecord().noDefault()
                    // array of records requires record schema to contain multiple fields
                    .name("myOptionalArraySubRecords").type().optional().array().items(SchemaBuilder.record("myOptionalArraySubRecord")
                            .fields().requiredBoolean("myRequiredBoolean").optionalString("myOptionalBoolean").endRecord())
                    .name("myNullableArraySubRecords").type().nullable().array().items(SchemaBuilder.record("myNullableArraySubRecord")
                            .fields().requiredBoolean("myRequiredString").optionalString("myOptionalString").endRecord()).noDefault()
                    .endRecord();

    private static GenericData.Record createGenericRecord(@SuppressWarnings("SameParameterValue") Schema schema) {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("myRequiredInt", 123);
        record.put("myRequiredString", "abc");
        record.put("myRequiredBoolean", true);
        record.put("myRequiredBytes", ByteBuffer.wrap("ABC123".getBytes()));
        record.put("myBytesDecimal", doubleToByteBuffer(1.23d));
        record.put("myRequiredTimestamp", System.currentTimeMillis() * 1000); // needs to be timestamp_micros (not timestamp_millis)
        record.put("myRequiredTime", System.currentTimeMillis() * 1000);
        record.put("myRequiredDateTime", LocalDateTime.now().toInstant(ZoneOffset.UTC).toEpochMilli() * 1000); // needs to be timestamp_micros (not timestamp_millis)
        record.put("myRequiredDate", (int) new Date(System.currentTimeMillis()).toLocalDate().toEpochDay());
        record.put("myRequiredArrayLongs", Arrays.asList(1L, 2L, 3L));
//        record.put("myRequiredArrayNullableInts", Arrays.asList(1, null, 3)); // requires conf.setBoolean("parquet.avro.write-old-list-structure", false);
        record.put("myRequiredArrayNullableInts", Arrays.asList(1, 3)); // requires conf.setBoolean("parquet.avro.write-old-list-structure", false);
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

    private static ByteBuffer doubleToByteBuffer(@SuppressWarnings("SameParameterValue") double d) {
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

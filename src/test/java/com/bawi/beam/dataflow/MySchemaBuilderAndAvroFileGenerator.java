package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class MySchemaBuilderAndAvroFileGenerator {
    public static void main(String[] args) throws IOException {
        Schema schema = SchemaBuilder.record("myRecordName").fields()
                .requiredInt("myRequiredInt") // requires value
                .name("myRequiredInt2").type().intType().noDefault() // requires value

                .optionalDouble("myOptionalDouble")
                .name("myOptionalDouble2").type().optional().doubleType()

                .nullableString("myNullableString", "myNullableStringDefaultValue")
                .name("myNullableString2").type().nullable().stringType().stringDefault("myNullableStringDefaultValue2")

                .name("myNullableRecord").type().optional().record("mySubRecord").fields().requiredBoolean("myBoolean").endRecord()
                .name("myRequiredSubRecord").type().record("_mySubRecord").fields().requiredFloat("myRequiredFloat").endRecord().noDefault()
                .name("myRequiredSubRecord2").type(SchemaBuilder.record("_myRequiredSubRecord").fields().requiredFloat("myRequiredFloat").endRecord()).noDefault()

                .name("myRequiredArrayLongs").type().array().items().longType().noDefault()
                .name("myOptionalArrayLongs").type().optional().array().items().longType()
                .name("myNullableArrayLongs").type().nullable().array().items().longType().noDefault()

                .name("myArraySubRecords").type().array().items(SchemaBuilder.record("myArraySubRecord").fields().requiredBoolean("myRequiredBoolean").endRecord()).noDefault()
                .name("myNullableArraySubRecords").type().nullable().array().items(SchemaBuilder.record("myNullableArraySubRecord").fields().requiredBoolean("myRequiredBoolean").endRecord()).noDefault()
                .name("myOptionalArraySubRecords").type().nullable().array().items(SchemaBuilder.record("myOptionalArraySubRecord").fields().requiredBoolean("myRequiredBoolean").endRecord()).noDefault()
                .endRecord();

        System.out.println(schema);

        File file = new File("my-file.avro");
        GenericData.Record record = new GenericData.Record(schema);

        record.put("myRequiredInt", 1);
        record.put("myRequiredInt2", 2);

        record.put("myOptionalDouble", 2.2d); // this line can be commented since optional long

        record.put("myNullableString", "abc"); // this line can be commented since optional string

        record.put("myRequiredArrayLongs", Arrays.asList(1L, 2L, 3L));  // required otherwise java.lang.NullPointerException: null of array in field myRequiredArrayLongs of myRecordName

        GenericData.Record myRequiredSubRecord = new GenericData.Record(schema.getField("myRequiredSubRecord").schema());
        myRequiredSubRecord.put("myRequiredFloat", 1.0f); // required otherwise null of int in field myRequiredFloat of myRequiredSubRecordType in field myRequiredSubRecord of myRecordName
        record.put("myRequiredSubRecord", myRequiredSubRecord); // required otherwise java.lang.NullPointerException: null of myRequiredSubRecordType in field myRequiredSubRecord of myRecordName
        record.put("myRequiredSubRecord2", myRequiredSubRecord); // required otherwise java.lang.NullPointerException: null of myRequiredSubRecordType in field myRequiredSubRecord of myRecordName

        GenericData.Record myArraySubRecord = new GenericData.Record(SchemaBuilder.record("myArraySubRecord").fields().requiredBoolean("myRequiredBoolean").endRecord());
        myArraySubRecord.put("myRequiredBoolean", true);
        record.put("myArraySubRecords", List.of(myArraySubRecord));
        GenericData.Record myOptionalArraySubRecord = new GenericData.Record(schema.getField("myOptionalArraySubRecords").schema().getTypes().get(0).getElementType());
        myOptionalArraySubRecord.put("myRequiredBoolean", true);
        record.put("myOptionalArraySubRecords", Arrays.asList(myOptionalArraySubRecord, myOptionalArraySubRecord));

        GenericDatumWriter<GenericRecord> genericDatumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(genericDatumWriter);
        dataFileWriter.create(schema, file);
        dataFileWriter.append(record);
        dataFileWriter.close();
    }
}
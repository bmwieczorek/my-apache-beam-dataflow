package com.bawi.beam.dataflow.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class AvroToBigQuerySchemaConverterTest {

    @Test
    public void convert() {
        // given
        TableSchema expected = getExpectedBigQueryTableSchema();

        // when
        TableSchema actual = AvroToBigQuerySchemaConverter.convert(getAvroSchema());

        // then
        Assert.assertEquals(expected, actual);
    }

    private TableSchema getExpectedBigQueryTableSchema() {
        TableFieldSchema requiredString = new TableFieldSchema().setName("myRequiredString").setType("STRING").setMode("REQUIRED");
        TableFieldSchema optionalInt = new TableFieldSchema().setName("myOptionalInt").setType("INTEGER").setMode("NULLABLE");
        TableFieldSchema requiredDateWithDoc = new TableFieldSchema().setName("myRequiredDate").setType("DATE").setMode("REQUIRED").setDescription("my docs");
        TableFieldSchema optionalTimestamp = new TableFieldSchema().setName("myOptionalTimestamp").setType("TIMESTAMP").setMode("NULLABLE");
        TableFieldSchema requiredRecord = new TableFieldSchema().setName("myRequiredRecord").setType("RECORD").setMode("REQUIRED");
        TableFieldSchema optionalLong = new TableFieldSchema().setName("myNullableLong").setType("INTEGER").setMode("NULLABLE");
        TableFieldSchema requiredBoolean = new TableFieldSchema().setName("myRequiredBoolean").setType("BOOLEAN").setMode("REQUIRED");
        requiredRecord.setFields(Arrays.asList(optionalLong, requiredBoolean));
        TableFieldSchema myRequiredArrayLongs = new TableFieldSchema().setName("myRequiredArrayLongs").setType("INTEGER").setMode("REPEATED");
        TableFieldSchema myOptionalArraySubRecords = new TableFieldSchema().setName("myOptionalArraySubRecords").setType("RECORD").setMode("REPEATED");
        TableFieldSchema myRequiredDouble = new TableFieldSchema().setName("myRequiredDouble").setType("FLOAT").setMode("REQUIRED");
        TableFieldSchema myOptionalFloat = new TableFieldSchema().setName("myOptionalFloat").setType("FLOAT").setMode("NULLABLE");
        myOptionalArraySubRecords.setFields(Arrays.asList(myRequiredDouble, myOptionalFloat));
        return new TableSchema().setFields(Arrays.asList(
                requiredString, optionalInt, requiredDateWithDoc, optionalTimestamp, requiredRecord, myRequiredArrayLongs, myOptionalArraySubRecords
        ));
    }

    private Schema getAvroSchema() {
        return SchemaBuilder.record("root")
                    .fields()
                    .requiredString("myRequiredString")
                    .optionalInt("myOptionalInt")
                    .name("myRequiredDate").doc("my docs").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                    .name("myOptionalTimestamp").type().optional().type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)))
                    .name("myRequiredRecord").type().record("myRecord")
                        .fields()
                            .nullableLong("myNullableLong", 1)
                            .requiredBoolean("myRequiredBoolean")
                        .endRecord().noDefault()
                    .name("myRequiredArrayLongs").type().array().items().longType().noDefault()
                    .name("myOptionalArraySubRecords").type().nullable().array()
                    .items(
                            SchemaBuilder.record("myOptionalArraySubRecordType")
                                .fields()
                                    .requiredDouble("myRequiredDouble")
                                    .optionalFloat("myOptionalFloat")
                                .endRecord()
                    ).noDefault()
                .endRecord();
    }
}
package com.bawi.beam.dataflow.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;

import java.util.ArrayList;
import java.util.List;

public class AvroToBigQuerySchemaConverter {
    public static TableSchema convert(Schema avroSchema) {
        TableSchema tableSchema = new TableSchema();
        List<TableFieldSchema> tableFieldSchemas = new ArrayList<>();
        for (Schema.Field field : avroSchema.getFields()) {
            tableFieldSchemas.add(getFieldSchema(field));
        }
        tableSchema.setFields(tableFieldSchemas);
        return tableSchema;
    }

    private static TableFieldSchema getFieldSchema(Schema.Field field) {
        return new TableFieldSchema()
                        .setName(field.name())
                        .setType(getTableFieldType(field.schema()))
                        .setMode(getTableFieldMode(field))
                        .setFields(getTableSubFields(field))
                        .setDescription(field.doc());
    }

    private static List<TableFieldSchema> getTableSubFields(Schema.Field field) {
        List<TableFieldSchema> tableSubFieldsSchemas = new ArrayList<>();
        Schema schema = field.schema();
        if (schema.getType() == Schema.Type.RECORD) {
            for (Schema.Field subField : schema.getFields()) {
                TableFieldSchema tableSubFieldSchema = getFieldSchema(subField);
                tableSubFieldsSchemas.add(tableSubFieldSchema);
            }
            return tableSubFieldsSchemas;
        }
        return null;
    }

    private static String getTableFieldType(Schema fieldSchema) {
        Schema.Type type = fieldSchema.getType();
        LogicalType logicalType = fieldSchema.getLogicalType();
        switch (type) {
            case RECORD:
                return "RECORD";
            case INT:
                return LogicalTypes.date().equals(logicalType)
                        ? "DATE" : "INTEGER";
            case LONG:
                return LogicalTypes.timestampMillis().equals(logicalType) || LogicalTypes.timestampMicros().equals(logicalType)
                        ? "TIMESTAMP" : "INTEGER";
            case BOOLEAN:
                return "BOOLEAN";
            case FLOAT:
            case DOUBLE:
                return "FLOAT";
            case BYTES:
                return "BYTES";
            case STRING:
                return "STRING";
            case UNION:
                return getTableFieldType(getUnionNotNullType(fieldSchema.getTypes()));
            default:
                throw new IllegalArgumentException("Unknown fieldSchema type: " + type);
        }
    }

    private static Schema getUnionNotNullType(List<Schema> types) {
        return Schema.Type.NULL != types.get(0).getType() ? types.get(0) : types.get(1);
    }

    private static String getTableFieldMode(Schema.Field field) {
        Schema schema = field.schema();
        return Schema.Type.UNION == schema.getType() ? "NULLABLE" : "REQUIRED";
    }
}

package com.bawi.beam.dataflow;

import com.bawi.beam.dataflow.bigquery.TableRowWithSchema;
import com.bawi.beam.dataflow.bigquery.TableRowWithSchemaCoder;
import com.bawi.beam.dataflow.schema.AvroToBigQuerySchemaConverter;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.json.gson.GsonFactory;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryAvroUtilsProxy;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.TableDestination;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

public class MyDynamicBQWriteJob {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyDynamicBQWriteJob.class);

    static class MyDoFn extends DoFn<KV<String, String>, TableRowWithSchema> {

        private Map<String, Schema> avroSchemaMapping;

        @Setup
        public void setup() {
            Schema optionalAvroSchema = SchemaBuilder.record("optional_record")
                    .fields()
                    .optionalString("myOptionalString")
//                .requiredInt("myOptionalInt")
                    .optionalLong("myOptionalInt")
                    .name("myOptionalDate").type().unionOf().nullType().and().type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).endUnion().noDefault()
                    .name("myOptionalTimestamp").type().unionOf().nullType().and().type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).endUnion().noDefault()
                    .optionalBoolean("myOptionalBoolean")
                    .endRecord();

            Schema requiredAvroSchema = SchemaBuilder.record("required_record")
                    .fields()
                    .requiredString("myRequiredString")
//                .requiredInt("myRequiredInt")
                    .requiredLong("myRequiredInt")
                    .name("myRequiredDate").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                    .name("myRequiredTimestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                    .requiredBoolean("myRequiredBoolean")
                    .endRecord();

            LOGGER.info("optionalAvroSchema={}", optionalAvroSchema.toString(true));
            LOGGER.info("requiredAvroSchema={}", requiredAvroSchema.toString(true));

            avroSchemaMapping = new HashMap<>();
            avroSchemaMapping.put(optionalAvroSchema.getName(), optionalAvroSchema);
            avroSchemaMapping.put(requiredAvroSchema.getName(), requiredAvroSchema);
        }

        @ProcessElement
        public void process(@Element KV<String, String> namedJson, OutputReceiver<TableRowWithSchema> outputReceiver) throws IOException {
            String key = namedJson.getKey();
            String json = namedJson.getValue();
            Schema avroSchema = avroSchemaMapping.get(key);
            GenericRecord record = new GenericData.Record(avroSchema);

            new ObjectMapper().<Map<String, Object>>readValue(json, new TypeReference<>(){}).forEach((k, v) -> {
                Schema.Field field = avroSchema.getField(k);
                Schema fieldSchema = field.schema();
                Schema.Type type = fieldSchema.getType();
                LogicalType logicalType = fieldSchema.getLogicalType();
                switch (type) {
                    case INT:
                        v = LogicalTypes.date().equals(logicalType) ? (int) LocalDate.parse((String) v).toEpochDay() : v;
                        break;
                    case LONG:
                        v = LogicalTypes.timestampMillis().equals(logicalType) || LogicalTypes.timestampMicros().equals(logicalType)
                                ? Instant.parse((String) v).toEpochMilli() * 1000 : (long) (int) v;
                }
                record.put(k, v);
            });

            TableSchema tableSchema = AvroToBigQuerySchemaConverter.convert(record.getSchema());

            tableSchema.setFactory(new GsonFactory()); // optional to set toString as json
            LOGGER.info("tableSchema={}", tableSchema.toPrettyString());

            TableRow tableRow = BigQueryAvroUtilsProxy.convertGenericRecordToTableRow(record, tableSchema);
            String tableName = record.getSchema().getName();
            TableRowWithSchema tableRowWithSchema = TableRowWithSchema.newBuilder().setDataType(tableName).setTableSchema(tableSchema)
                    .setTableRow(tableRow).build();
            LOGGER.info("tableRowWithSchema={}", tableRowWithSchema);
            outputReceiver.output(tableRowWithSchema);
        }
    }

    static class MyDynamicBQWritePTransform extends PTransform<PBegin, PCollection<TableRowWithSchema>> {

        @Override
        public PCollection<TableRowWithSchema> expand(PBegin input) {
/*
            GenericRecord optionalRecord = new GenericData.Record(optionalAvroSchema);
            optionalRecord.put("myOptionalString", "abc");

            LOGGER.info("optionalRecord={}", optionalRecord);


            Schema requiredAvroSchema = SchemaBuilder.record("required_record")
                    .fields()
                    .optionalString("myRequiredString")
//                .requiredInt("myRequiredInt")
                    .requiredLong("myRequiredInt")
                    .name("myRequiredDate").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                    .name("myRequiredTimestamp").type(LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                    .endRecord();


            GenericRecord requiredRecord = new GenericData.Record(requiredAvroSchema);
            requiredRecord.put("myRequiredString", "abc");
//        requiredRecord.put("myRequiredInt", 123);
            requiredRecord.put("myRequiredInt", 123L);
            requiredRecord.put("myRequiredDate", (int) LocalDate.parse("2017-01-01").toEpochDay());
            requiredRecord.put("myRequiredTimestamp", Instant.parse("2022-03-20T03:41:42.123Z").toEpochMilli() * 1000);

            LOGGER.info("requiredRecord={}", requiredRecord);*/

            return input.apply(Create.of(
                            KV.of("optional_record", "{\"myOptionalString\": \"abc\"}"),
                            KV.of("required_record", "{\"myRequiredString\": \"abc\", \"myRequiredInt\": 123, \"myRequiredDate\": \"2017-01-01\", \"myRequiredTimestamp\": \"2022-03-20T03:41:42.123Z\", \"myRequiredBoolean\": true}")
                        )
                    )
                    .apply(ParDo.of(new MyDoFn())).setCoder(TableRowWithSchemaCoder.of());
        }
    }

    public interface MyPipelineOptions extends PipelineOptions {

        @Validation.Required
        ValueProvider<String> getBqLoadProjectId();
        void setBqLoadProjectId(ValueProvider<String> inputPath);

        @Validation.Required
        ValueProvider<String> getDataset();
        void setDataset(ValueProvider<String> dataset);
    }

    public static void main(String[] args) {
        MyPipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(MyPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);

        ValueProvider<String> dataset = options.getDataset();
        ValueProvider<String> bqLoadProjectId = options.getBqLoadProjectId();

        pipeline.apply(new MyDynamicBQWritePTransform())
                .apply(BigQueryIO.<TableRowWithSchema>write()
                        .to((ValueInSingleWindow<TableRowWithSchema> input) -> getTableDestination(input, bqLoadProjectId, dataset))
                        .withFormatFunction(TableRowWithSchema::getTableRow)
                        .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                        .withLoadJobProjectId(bqLoadProjectId)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER));

        pipeline.run();
    }

    private static TableDestination getTableDestination(ValueInSingleWindow<TableRowWithSchema> input, ValueProvider<String> bqLoadProjectId, ValueProvider<String> dataset) {
        TableRowWithSchema tableRowWithSchema = input.getValue();
        String tableName = tableRowWithSchema.getDataType();
        TableReference tableReference = new TableReference();
        tableReference.setProjectId(bqLoadProjectId.get());
        tableReference.setDatasetId(dataset.get());
        tableReference.setTableId(tableName);
        return new TableDestination(tableReference, null);
    }
}

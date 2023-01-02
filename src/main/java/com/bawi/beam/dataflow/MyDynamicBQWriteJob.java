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
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
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

                    // int type needs to saved as avro long to be mapped to INTEGER/INT64 in BQ
                    .optionalLong("myOptionalInt")

                    // date stored as string needs to saved as avro int with date logical type to be mapped to DATE in BQ
                    .name("myOptionalDate").type().unionOf().nullType().and().type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).endUnion().noDefault()

                    // timestamp stored as string needs to saved as avro long with timestampMicros logic type to be mapped to TIMESTAMP in BQ
                    .name("myOptionalTimestamp").type().unionOf().nullType().and().type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).endUnion().noDefault()
                    .optionalBoolean("myOptionalBoolean")

                    // double type needs to saved to as avro bytes with decimal logical type to be mapped to NUMERIC in BQ
                    .name("myOptionalNumeric").type().unionOf().nullType().and().type(LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES))).endUnion().noDefault()

                    // double type will be mapped to FLOAT/FLOAT64 in BQ
                    .optionalDouble("myOptionalDouble")

                    // time stored as string needs to saved as avro long with timeMicros logic type to be mapped to TIME in BQ
                    .name("myOptionalTime").type().unionOf().nullType().and().type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))).endUnion().noDefault()

                    // bytes are converted to ByteBuffer and then to base64 encoded string stores as BYTES in BQ, queried as SELECT SAFE_CONVERT_BYTES_TO_STRING(myRequiredBytes)
                    .optionalBytes("myOptionalBytes")

                    // float needs to be saved as avro double to be mapped to FLOAT/FLOAT64 in BQ
                    .optionalDouble("myOptionalFloat")

                    .optionalLong("myOptionalLong")
                    .endRecord();

            Schema requiredAvroSchema = SchemaBuilder.record("required_record")
                    .fields()
                    .requiredString("myRequiredString")
                    .requiredLong("myRequiredInt")
                    .name("myRequiredDate").type(LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT))).noDefault()
                    .name("myRequiredTimestamp").type(LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                    .requiredBoolean("myRequiredBoolean")
                    .name("myRequiredNumeric").type(LogicalTypes.decimal(38, 9).addToSchema(Schema.create(Schema.Type.BYTES))).noDefault()
                    .requiredDouble("myRequiredDouble")
                    .name("myRequiredTime").type(LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG))).noDefault()
                    .requiredBytes("myRequiredBytes")
                    .requiredDouble("myRequiredFloat")
                    .requiredLong("myRequiredLong")
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

            new ObjectMapper().<Map<String, Object>>readValue(json, new TypeReference<>(){}).forEach((String fieldName, Object value) -> {
                value = convertValue(value, avroSchema.getField(fieldName));
                record.put(fieldName, value);
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

        private Object convertValue(Object value, Schema.Field field) {
            Schema fieldSchema = field.schema();
            Schema.Type type = fieldSchema.getType();
            LogicalType logicalType = fieldSchema.getLogicalType();
            switch (type) {
                case INT:
                    value = LogicalTypes.date().equals(logicalType) ? (int) LocalDate.parse((String) value).toEpochDay() : value;
                    break;
                case LONG:
                    value = LogicalTypes.timestampMicros().equals(logicalType) || LogicalTypes.timestampMillis().equals(logicalType) ?
                            Instant.parse((String) value).toEpochMilli() * 1000 :
                            LogicalTypes.timeMicros().equals(logicalType) || LogicalTypes.timeMillis().equals(logicalType) ?
                                    LocalTime.parse((String) value).toNanoOfDay() / 1000 :
                                    ((Number) value).longValue();
                    break;
                case BYTES:
                    value = LogicalTypes.decimal(38, 9).equals(logicalType) ? parseNumeric((double) value) : ByteBuffer.wrap(((String) value).getBytes());
            }
            return value;
        }

        private ByteBuffer parseNumeric(double d) {
            BigDecimal bigDecimal = BigDecimal.valueOf(d).setScale(9, RoundingMode.UNNECESSARY);
            BigInteger bigInteger = bigDecimal.unscaledValue();
            return ByteBuffer.wrap(bigInteger.toByteArray());
        }
    }

    static class MyDynamicBQWritePTransform extends PTransform<PBegin, PCollection<TableRowWithSchema>> {

        @Override
        public PCollection<TableRowWithSchema> expand(PBegin input) {

            return input.apply(Create.of(
                            KV.of("optional_record", "{\"myOptionalString\": \"abc\"}"),
                            KV.of("required_record", "{" +
                                    "\"myRequiredString\": \"abc\", " +
                                    "\"myRequiredInt\": 123, " +
                                    "\"myRequiredDate\": \"2017-01-01\", " +
                                    "\"myRequiredTimestamp\": \"2022-03-20T03:41:42.123Z\", " +
                                    "\"myRequiredBoolean\": true, " +
                                    "\"myRequiredNumeric\": 1.23, " +
                                    "\"myRequiredDouble\": 4.56, " +
                                    "\"myRequiredTime\": \"12:34:56.789\", " +
                                    "\"myRequiredBytes\": \"abc123\", " +
                                    "\"myRequiredFloat\": 7.89, " +
                                    "\"myRequiredLong\": 567" +
                                    "}")
                        )
                    )
                    .apply(ParDo.of(new MyDoFn())).setCoder(TableRowWithSchemaCoder.of());
        }
    }

    @SuppressWarnings("unused")
    public interface MyPipelineOptions extends PipelineOptions {

        @Validation.Required
        ValueProvider<String> getBqLoadProjectId();
        void setBqLoadProjectId(ValueProvider<String> inputPath);

        @Validation.Required
        ValueProvider<String> getDataset();
        void setDataset(ValueProvider<String> dataset);
    }

    public static void main(String[] args) {
        args = PipelineUtils.updateArgsWithDataflowRunner(args, "--bqLoadProjectId=" + System.getenv("GCP_PROJECT"),
                "--dataset=" + System.getenv("GCP_OWNER") + "_" + MyDynamicBQWriteJob.class.getSimpleName().toLowerCase());

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

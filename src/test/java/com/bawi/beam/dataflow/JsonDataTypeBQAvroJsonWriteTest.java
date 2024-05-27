package com.bawi.beam.dataflow;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryOptions;
import org.apache.beam.sdk.io.gcp.bigquery.SchemaAndRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

public class JsonDataTypeBQAvroJsonWriteTest implements Serializable {
    static String project = System.getenv("GCP_PROJECT");

    @Test
    public void test() throws IOException {
        String owner = System.getenv("GCP_OWNER");
        String dataset = owner + "_" + "dataset";
        String table = "my_json_table";
        String gcsTempLocation = "gs://" + project + "_" + owner + "/temp";

        writePipeline
            .apply(Create.of(
                    """
                    {"user_id": "id1", "event_properties": {"myStrProp1": "myVal1", "myIntProp1": 10}}
                    """,
                    """
                    {"user_id": "id2", "event_properties": {"myStrProp2": "myVal2", "myIntProp1": 20}}
                    """
            ))
            .apply(ParDo.of(new DoFn<String, GenericRecord>() {
                @ProcessElement
                public void process(@Element String jsonString, OutputReceiver<GenericRecord> receiver) {
                    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);

                    //  optionally & manually
//                    record.put("user_id", "id1");
//                    JsonObject jsonObject = new JsonObject();
//                    jsonObject.addProperty("myStrProp1", "myVal123");
//                    jsonObject.addProperty("myIntProp1", 123567);
//                    record.put("event_properties", jsonObject.toString());

                    JsonObject jsonObj = GSON.fromJson(jsonString, JsonObject.class);
                    record.put("user_id", jsonObj.get("user_id").getAsString());
                    record.put("event_properties", GSON.toJson(jsonObj.get("event_properties")));
                    receiver.output(record);
                }
            }))
            .setCoder(AvroGenericCoder.of(AVRO_SCHEMA))
            .apply(BigQueryIO.<GenericRecord>write()
                    .to(String.format("%s.%s.%s", project, dataset, table))
                    .withAvroFormatFunction(AvroWriteRequest::getElement)
                    .withAvroSchemaFactory(qTableSchema -> AVRO_SCHEMA)
                    .withJsonSchema(BQ_TABLE_SCHEMA) // bq schema type json needs to be lower case
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

        PipelineOptions options = TestPipeline.testingPipelineOptions();
        options.setTempLocation(gcsTempLocation);
        writePipeline.run(options).waitUntilFinish();


        BigQueryOptions bigQueryOptions = TestPipeline.testingPipelineOptions().as(BigQueryOptions.class);
        bigQueryOptions.setProject(project);
        bigQueryOptions.setTempLocation(gcsTempLocation);

        String query = String.format(
                "SELECT user_id, event_properties FROM %s.%s.%s WHERE SAFE.INT64(event_properties.myIntProp1) = %d",
                project, dataset, table, 10);

        PCollection<String> retrievedRecordsUserIds = readPipeline.apply(BigQueryIO.read(SchemaAndRecord::getRecord)
                        .fromQuery(query)
                        .usingStandardSql() // required, otherwise invalidQuery - needs to be below .fromQuery
                        .withCoder(AvroGenericCoder.of(AVRO_SCHEMA))
                        .withQueryTempDataset(dataset))
                .apply(MapElements.into(TypeDescriptors.strings()).via(record -> record.get("user_id").toString()));

        PAssert.that(retrievedRecordsUserIds).containsInAnyOrder("id1");

        readPipeline.run(bigQueryOptions).waitUntilFinish();
    }

    private static final Gson GSON = new Gson();

    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
            """
                    {
                       "type" : "record",
                       "name" : "myRecord",
                       "fields" : [
                         { "name" : "user_id", "type" : "string" },
                         { "name" : "event_properties", "type" : { "type" : "string", "sqlType" : "JSON" }}
                      ]
                    }
                """);

    private static final String BQ_TABLE_SCHEMA =
            """
                {
                  "fields": [
                    { "name": "user_id", "type": "STRING" },
                    { "name": "event_properties", "type": "json" }
                  ]
                }
            """;

    @Rule
    public final transient TestPipeline writePipeline = TestPipeline.create();

    @Rule
    public final transient TestPipeline readPipeline = TestPipeline.create();
}

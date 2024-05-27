package com.bawi.beam.dataflow;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

public class JsonDataTypeBQAvroJsonWriteTest implements Serializable {

    private static final Schema AVRO_SCHEMA = createSchema();
    private static final Gson GSON = new Gson();

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws IOException {

        pipeline
            .apply(Create.of(
                    """
                    {"user_id": "id123", "event_properties": {"myStrProp1": "myVal1", "myIntProp1": 1000}}
                    """
                    ))
            .apply(ParDo.of(new DoFn<String, GenericRecord>() {
                @ProcessElement
                public void process(@Element String jsonString, OutputReceiver<GenericRecord> receiver) {
                    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
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
            })).setCoder(AvroGenericCoder.of(AVRO_SCHEMA))
            .apply(BigQueryIO.<GenericRecord>write()
                    .withAvroFormatFunction(AvroWriteRequest::getElement)
                    .withAvroSchemaFactory(qTableSchema -> AVRO_SCHEMA)
                    .to(System.getenv("GCP_PROJECT") + ":bartek_person.my_json_table9")
                    // bq schema type json needs to be lower case
                    .withJsonSchema(
                            """
                            {
                              "fields": [
                                {
                                  "name": "user_id",
                                  "type": "STRING"
                                },
                                {
                                  "name": "event_properties",
                                  "type": "json"
                                }
                              ]
                            }
                            """)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                    .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        PipelineOptions options = TestPipeline.testingPipelineOptions();
        options.setTempLocation("gs://" + System.getenv("GCP_PROJECT") + "_bartek_us/temp");
        pipeline.run(options);
    }

    private static Schema createSchema() {
        Schema jsonStringSchema = Schema.create(Schema.Type.STRING);
        jsonStringSchema.addProp("sqlType", "json");
        return SchemaBuilder
                .record("myRecord")
                .fields()
                .optionalString("user_id")
                .name("event_properties").type(jsonStringSchema).noDefault()
                .endRecord();
    }
}

//bq query --use_legacy_sql=false "SELECT * FROM ${GCP_PROJECT}.bartek_person.my_json_table9 WHERE JSON_EXTRACT_SCALAR(event_properties, '$.myIntProp1') = '123'"
//+---------+------------------------------------------+
//| user_id |             event_properties             |
//+---------+------------------------------------------+
//| id1     | {"myIntProp1":123,"myStrProp1":"myVal1"} |
//+---------+------------------------------------------+


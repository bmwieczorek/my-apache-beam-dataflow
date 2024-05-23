package com.bawi.beam.dataflow;

import com.google.api.services.bigquery.model.TableRow;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

public class JsonDataTypeBQTableRowWriteTest implements Serializable {
    private static final Schema SCHEMA = createSchema();

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws IOException {
            System.out.println(SCHEMA.toString(true));

            pipeline.apply(Create.of(
                        """
                        { "name": "John", "age": 18 }
                        """,
                """
                        { "name": "Billy", "age": 20 }
                        """
            ))
            .apply(ParDo.of(new DoFn<String, TableRow>() {
                @ProcessElement
                public void process(@Element String json, OutputReceiver<TableRow> receiver) {
//                    JsonObject jsonObject = new JsonObject();
//                    jsonObject.addProperty("name", "John");
//                    Gson gson = new Gson();
//                    String json = gson.toJson(jsonObject);
                    receiver.output(new TableRow().set("json_field", json));
                }
            }))
            .apply(BigQueryIO.writeTableRows()
                    .to("project:dataset.table")
                    .withJsonSchema(
                            """
                            {
                              "fields": [
                                {
                                  "name": "json_field",
                                  "type": "JSON"
                                }
                              ]
                            }
                            """)
                    .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//                        .withMethod(BigQueryIO.Write.Method.FILE_LOADS)
//                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                    .withMethod(BigQueryIO.Write.Method.STORAGE_WRITE_API)
                    .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
        pipeline.run();
    }

    private static Schema createSchema() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        stringSchema.addProp("sqlType", "JSON");
        return SchemaBuilder.record("myRecord")
                .fields()
                .name("json_field").type(stringSchema).noDefault()
                .endRecord();
    }
}

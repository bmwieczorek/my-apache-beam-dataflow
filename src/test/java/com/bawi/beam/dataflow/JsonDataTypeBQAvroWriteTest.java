package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.gcp.bigquery.AvroWriteRequest;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;

public class JsonDataTypeBQAvroWriteTest implements Serializable {
    private static final Schema AVRO_SCHEMA = new Schema.Parser().parse(
            """
                    {
                       "type" : "record",
                       "name" : "myRecord",
                       "fields" : [
                        {
                         "name" : "json_field",
                         "type" : {
                           "type" : "string",
                           "sqlType" : "JSON"
                         }
                       }
                      ]
                    }
                """);

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws IOException {
            System.out.println(AVRO_SCHEMA.toString(true));

            pipeline.apply(Create.of(
                        """
                        { "name": "John2", "age": 180 }
                        """,
                """
                        { "name": "Billy2", "age": 200 }
                        """
            ))
            .apply(ParDo.of(new DoFn<String, GenericRecord>() {
                @ProcessElement
                public void process(@Element String json, OutputReceiver<GenericRecord> receiver) {
                    GenericRecord record = new GenericData.Record(AVRO_SCHEMA);
                    record.put("json_field", json);
                    receiver.output(record);
                }
            })).setCoder(AvroGenericCoder.of(AVRO_SCHEMA))
            .apply(BigQueryIO.<GenericRecord>write()
                    .withAvroFormatFunction(AvroWriteRequest::getElement)
                    .withAvroSchemaFactory(qTableSchema -> AVRO_SCHEMA)
                    //.to("project:dataset.table")
                    .to(System.getenv("GCP_PROJECT") + ":" + System.getenv("GCP_OWNER") + "_person.my_json_table")
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

}

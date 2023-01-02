package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.io.parquet.ParquetIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class ReadParquetFileTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws IOException {
        // given - generate file
        Schema schema = SchemaBuilder.record("myRecord").fields().requiredString("name").requiredBytes("body").endRecord();
        GenericData.Record record = new GenericData.Record(schema);
        record.put("name", "Bob");
        record.put("body", ByteBuffer.wrap("abc".getBytes()));

        Path path = new Path("target/myRecord.parquet");
        File file = new File(path.toUri().getPath());
        deleteIfPresent(file);

        try (ParquetWriter<GenericData.Record> writer =
             AvroParquetWriter.<GenericData.Record>builder(HadoopOutputFile.fromPath(path,new Configuration()))
                .withSchema(schema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .build()) {

                writer.write(record);
        }

        // when - read file
        PCollection<String> pCollection = pipeline.apply(
            ParquetIO
                .parseGenericRecords(new SerializableFunction<GenericRecord, String>() { // need anonymous type to infer output type
                    @Override
                    public String apply(GenericRecord genericRecord) {
                        Utf8 name = (Utf8) genericRecord.get("name");
                        ByteBuffer byteBuffer = (ByteBuffer) genericRecord.get("body");
                        byte[] bytes = byteBuffer.array();
                        return name.toString() + "," + new String(bytes);
                    }
                })
                .from("target/myRecord.parquet")
        );

        // assert
        PAssert.thatSingleton(pCollection).isEqualTo("Bob,abc");
        pipeline.run().waitUntilFinish();
        deleteIfPresent(file);
    }

    private static void deleteIfPresent(File file) {
        if (file.exists()) {
            //noinspection ResultOfMethodCallIgnored
            file.delete();
        }
    }
}

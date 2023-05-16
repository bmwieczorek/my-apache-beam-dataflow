package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;

public class ReadAvroFileTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws IOException {
        // given - generate file
        Schema schema = SchemaBuilder.record("myRecord").fields().requiredString("name").requiredBytes("body").endRecord();
//        String pathname = "target/myRecord-1k.snappy.avro";
        String pathname = "target/myRecord.snappy.avro";
        File avroFile = new File(pathname);
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.snappyCodec());
        dataFileWriter.create(schema, avroFile);

//        for (int i = 0; i < 1000; i++) {
        for (int i = 0; i < 1; i++) {
            GenericRecord record = new GenericData.Record(schema);
            record.put("name", "Bob");
            record.put("body", ByteBuffer.wrap("abc".getBytes()));
            dataFileWriter.append(record);
        }

        dataFileWriter.close();

//        byte[] bytes = Files.readAllBytes(Path.of(pathname));
//        byte[] out = new byte[800000];
//        Snappy.compress(bytes, 0, 8, out, 0);
//        String pathname2 = "target/myRecord.snappy.avro";
//        Files.write(Path.of(pathname2), out);

        // when - read file
        PCollection<String> pCollection = pipeline.apply(
            AvroIO
                .parseGenericRecords(new SerializableFunction<GenericRecord, String>() { // need anonymous type to infer output type
                    @Override
                    public String apply(GenericRecord genericRecord) {
                        Utf8 name = (Utf8) genericRecord.get("name");
                        ByteBuffer byteBuffer = (ByteBuffer) genericRecord.get("body");
                        byte[] bytes = byteBuffer.array();
                        return name.toString() + "," + new String(bytes);
                    }
                })
                .from(pathname)
        ).apply(ParDo.of(new MyBundleSizeInterceptor<>("")));

        // assert
        PAssert.thatSingleton(pCollection).isEqualTo("Bob,abc");
        pipeline.run().waitUntilFinish();
    }
}

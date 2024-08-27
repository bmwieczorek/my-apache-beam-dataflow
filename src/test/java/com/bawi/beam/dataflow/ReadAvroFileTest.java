package com.bawi.beam.dataflow;

import com.bawi.beam.WindowUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.UUID;

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
            GenericRecord record = createRecord(schema, "Bob", "abc");
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

    @Test
    public void testWindowedWrites() {
        String classSimpleName = ReadAvroFileTest.class.getSimpleName();
        Schema schema = SchemaBuilder.record("myRecord").fields().requiredString("name").requiredBytes("body").endRecord();

        ValueProvider.StaticValueProvider<String> output = ValueProvider.StaticValueProvider.of("target/" + classSimpleName + "/output/");
        ValueProvider.StaticValueProvider<String> temp = ValueProvider.StaticValueProvider.of("target/" + classSimpleName + "/temp");


        pipeline.apply(Create.timestamped(
                        TimestampedValue.of(createRecord(schema, "Bob", "bob"), Instant.now()),
                        TimestampedValue.of(createRecord(schema, "Bob2", "bob2"), Instant.now().plus(Duration.standardSeconds(1)))
                    ).withCoder(AvroCoder.of(schema))
                )
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(1))))
                .apply(AvroIO
                        .writeGenericRecords(schema)
                        .withWindowedWrites()

                        // writes as target/ReadAvroFileTest/GlobalWindow-pane-0-last-00000-of-00001 or target/ReadAvroFileTest/2023-05-20T06:16:22.000Z-2023-05-20T06:16:23.000Z-pane-0-last-00001-of-00002
                        //.to(FileBasedSink.convertToFileResourceIfPossible("target/" + classSimpleName + "/").getCurrentDirectory()));
                        .to(new FileBasedSink.FilenamePolicy() {
                            @Override
                            public ResourceId windowedFilename(int shardNumber, int numShards, BoundedWindow window, PaneInfo paneInfo, FileBasedSink.OutputFileHints outputFileHints) {
                                ResourceId resource = FileBasedSink.convertToFileResourceIfPossible(output.get());
                                String parentDirectoryPathWithSlash =  resource.isDirectory() ? "" : resource.getFilename() + "/";
                                String filename = String.format("%s%s-%s-%s-of-%s%s", parentDirectoryPathWithSlash, UUID.randomUUID(),
                                        WindowUtils.windowToNormalizedString(window), shardNumber, numShards, ".avro");
                                ResourceId resourceId = resource.getCurrentDirectory().resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
                                System.out.println(filename + "," + resourceId);
                                // "target/" + classSimpleName + "/output/" -> c8...91-2023-05-20T08_16_49.000Z..2023-05-20T08_16_50.000Z-1-of-2.avro,/Users/me/dev/my-apache-beam-dataflow/target/ReadAvroFileTest/output/c8...91-2023-05-20T08_16_49.000Z..2023-05-20T08_16_50.000Z-1-of-2.avro
                                // "target/" + classSimpleName + "/output" -> output/31...00-2023-05-20T08_22_50.000Z..2023-05-20T08_22_51.000Z-1-of-2.avro,/Users/me/dev/my-apache-beam-dataflow/target/ReadAvroFileTest/output/31...00-2023-05-20T08_22_50.000Z..2023-05-20T08_22_51.000Z-1-of-2.avro
                                return resourceId;
                            }

                            @Override
                            public ResourceId unwindowedFilename(int shardNumber, int numShards, FileBasedSink.OutputFileHints outputFileHints) {
                                return null;
                            }
                        })
                        // with FilenamePolicy use either .to(output) or .withTempDirectory() to determine directory with temp files
//                        .to(output)
                        .withTempDirectory(ValueProvider.NestedValueProvider.of(temp, FileBasedSink::convertToFileResourceIfPossible))
                    );

        pipeline.run().waitUntilFinish();
    }

    private static GenericRecord createRecord(Schema schema, String name, String body) {
        GenericRecord genericRecord = new GenericData.Record(schema);
        genericRecord.put("name", name);
        genericRecord.put("body", ByteBuffer.wrap(body.getBytes()));
        return genericRecord;
    }
}

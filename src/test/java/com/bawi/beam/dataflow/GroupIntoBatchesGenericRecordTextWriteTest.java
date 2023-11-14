package com.bawi.beam.dataflow;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroGenericCoder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.bawi.beam.dataflow.LogUtils.windowToString;

public class GroupIntoBatchesGenericRecordTextWriteTest {
    private static final DateTimeFormatter Y_M_D_H_M_FORMATTER = DateTimeFormat.forPattern("'year='yyyy/'month'=MM/'day'=dd/'hour'=HH/mm");

    private static final Schema SCHEMA = SchemaBuilder.record("myRecord")
            .fields().requiredString("myTimestampString").optionalInt("myInt").endRecord();

    private static final List<GenericRecord> RECORDS = Arrays.asList(
            // W1 [0-5) s
            createGenericRecord("2021-12-16T00:00:00Z", 1),
            createGenericRecord("2021-12-16T00:00:01Z", 2),
            createGenericRecord("2021-12-16T00:00:02Z", 3),

            // W2 [5,10) s
            createGenericRecord("2021-12-16T00:01:05Z", 10),
            createGenericRecord("2021-12-16T00:01:06Z", 20),
            createGenericRecord("2021-12-16T00:01:07Z", 30),
            createGenericRecord("2021-12-16T00:01:07Z", 40),
            createGenericRecord("2021-12-16T00:01:07Z", 50),
            createGenericRecord("2021-12-16T00:01:08Z", 60),
            createGenericRecord("2021-12-16T00:01:09Z", 70)
    );

    @Test
    public void shouldGroupKVsIntoBatches() {
        Pipeline pipeline = Pipeline.create();

        PCollection<GenericRecord> records = pipeline.apply(Create.of(RECORDS)
                .withCoder(AvroGenericCoder.of(SCHEMA)));

        PCollection<KV<String, GenericRecord>> withMinuteDateTimeKey = records
                .apply(ParDo.of(new WithMinuteDateTimeKey()))
//                .apply(ParDo.of(new LogElementWithWindowDetails<>("1")))
                ;

        PCollection<KV<String, Iterable<GenericRecord>>> batched = withMinuteDateTimeKey.apply(GroupIntoBatches.ofSize(3));

        PCollection<KV<String, GenericRecord>> flattenedWithBatchKey = batched.apply(ParDo.of(new FlattenGroupWithSameKey()))
//                .apply(ParDo.of(new LogElementWithWindowDetails<>("2")))
                ;

        flattenedWithBatchKey
                .apply(FileIO.<String, KV<String, GenericRecord>>writeDynamic()
                        .by(KV::getKey)
                        .via(Contextful.fn(kv -> kv.getValue().toString()), TextIO.sink())
                        .withDestinationCoder(StringUtf8Coder.of())
                        .withNaming(subPath -> new MyFileNaming(subPath, ".txt"))
                        .to(OUTPUT_DIR)
                        .withTempDirectory(TEMP_DIR)
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();

    }

    private static class WithMinuteDateTimeKey extends DoFn<GenericRecord, KV<String, GenericRecord>> {

        @ProcessElement
        public void process(@Element GenericRecord element, OutputReceiver<KV<String, GenericRecord>> receiver) {
            Utf8 key = (Utf8) element.get("myTimestampString");
            GenericRecord record = createGenericRecord(key.toString(), (int) element.get("myInt"));
            Instant instant = Instant.parse(key.toString());
            String minuteDateTimeKey = Y_M_D_H_M_FORMATTER.print(instant.getMillis());
            receiver.output(KV.of(minuteDateTimeKey, record));
        }
    }

    private static GenericRecord createGenericRecord(String myTimestampString, Integer myInt) {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("myTimestampString", myTimestampString);
        record.put("myInt", myInt);
        return record;
    }

    private static class FlattenGroupWithSameKey extends DoFn<KV<String, Iterable<GenericRecord>>, KV<String, GenericRecord>> {

        @ProcessElement
        public void process(@Element KV<String, Iterable<GenericRecord>> element, OutputReceiver<KV<String, GenericRecord>> receiver, @Timestamp Instant ts) {
            String key = element.getKey() + "/" + UUID.randomUUID();
            for (GenericRecord record : element.getValue()) {
                KV<String, GenericRecord> output = KV.of(key, record);
                LOGGER.info("Processing {}", output);
                receiver.output(output);
            }
        }
    }

    private static final String OUTPUT_DIR = Paths.get("target", GroupIntoBatchesGenericRecordTextWriteTest.class.getSimpleName(), "output").toAbsolutePath().toString();
    private static final String TEMP_DIR = Paths.get("target", GroupIntoBatchesGenericRecordTextWriteTest.class.getSimpleName(), "temp").toAbsolutePath().toString();

    @Before
    public void setup() throws IOException {
        deleteDirectoryContentsIfExists(OUTPUT_DIR);
        deleteDirectoryContentsIfExists(TEMP_DIR);
    }

    static class MyFileNaming implements FileIO.Write.FileNaming {
        private final String subPath;
        private final String extension;

        public MyFileNaming(String subPath, String extension) {
            this.subPath = subPath;
            this.extension = extension;
        }

        @Override
        public String getFilename(BoundedWindow w, PaneInfo p, int numShards, int shardIndex, Compression compression) {
            String filename = String.format("%s-winMaxTs-%s-shard-%s-of-%s%s", subPath, w.maxTimestamp().toString().replace(":","_").replace(" ","_"), shardIndex, numShards, extension);
            LOGGER.info("[{}][Write] Writing data to w={},p={}", filename, windowToString(w), p);
            return filename;
        }
    }

    private void deleteDirectoryContentsIfExists(String directory) throws IOException {
        Path directoryPath = Paths.get(directory);
        if (Files.exists(directoryPath)) {
            try (Stream<Path> paths = Files.walk(directoryPath)) {
                Stream<File> fileStream = paths.sorted(Comparator.reverseOrder()).map(Path::toFile);
                //noinspection ResultOfMethodCallIgnored
                fileStream.forEach(File::delete);
            }
        }
        Files.createDirectories(directoryPath);
    }

    private static class LogElementWithWindowDetails<T> extends DoFn<T, T> {
        private final String label;

        private LogElementWithWindowDetails(String label) {
            this.label = label;
        }

        @ProcessElement
        public void process(@Element T element, OutputReceiver<T> outputReceiver, @Timestamp Instant timestamp, BoundedWindow window, PaneInfo paneInfo) {
            String windowString = window instanceof GlobalWindow ? "GlobalWindow: maxTimestamp=" + window.maxTimestamp() : window.getClass().getSimpleName() + ": " + window;
            String msg = String.format("[" + label + "] Processing '%s',ts=%s,w=%s,p=%s", element, timestamp, windowString, paneInfo);
            LOGGER.info(msg);
            outputReceiver.output(element);
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupIntoBatchesGenericRecordTextWriteTest.class);

}

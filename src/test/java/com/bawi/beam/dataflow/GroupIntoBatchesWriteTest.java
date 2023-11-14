package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
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
import java.util.Comparator;
import java.util.UUID;
import java.util.stream.Stream;

import static com.bawi.beam.dataflow.LogUtils.windowToString;

public class GroupIntoBatchesWriteTest {
    private static final DateTimeFormatter Y_M_D_H_M_FORMATTER = DateTimeFormat.forPattern("'year='yyyy/'month'=MM/'day'=dd/'hour'=HH/mm");

    private static final Create.TimestampedValues<KV<String, Integer>> TIMESTAMPED_KV_EVENTS = Create.timestamped(
        // W1 [0-5) s
        TimestampedValue.of(KV.of("a", 1), Instant.parse("2021-12-16T00:00:00Z")), // batch 1
        TimestampedValue.of(KV.of("a", 2), Instant.parse("2021-12-16T00:00:01Z")), // batch 1
        TimestampedValue.of(KV.of("b", 3), Instant.parse("2021-12-16T00:00:02Z")), // batch 2

        // W2 [5,10) s
        TimestampedValue.of(KV.of("b", 10), Instant.parse("2021-12-16T00:00:05Z")), // batch 6 or 7
        TimestampedValue.of(KV.of("c", 20), Instant.parse("2021-12-16T00:00:06Z")), // batch 3
        TimestampedValue.of(KV.of("a", 30), Instant.parse("2021-12-16T00:00:07Z")), // batch 4
        TimestampedValue.of(KV.of("b", 40), Instant.parse("2021-12-16T00:00:07Z")), // batch 5
        TimestampedValue.of(KV.of("d", 50), Instant.parse("2021-12-16T00:00:07Z")), // batch 6 or 7
        TimestampedValue.of(KV.of("b", 60), Instant.parse("2021-12-16T00:00:08Z")), // batch 6 or 7
        TimestampedValue.of(KV.of("b", 70), Instant.parse("2021-12-16T00:00:09Z"))  // batch 6 or 7
    );

    @Test
    public void shouldGroupKVsIntoBatches() {
        Pipeline pipeline = Pipeline.create();

        PCollection<KV<String, Integer>> windowed =
            pipeline.apply(TIMESTAMPED_KV_EVENTS)
                    .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));

        PCollection<KV<String, Iterable<Integer>>> batched = windowed.apply(GroupIntoBatches.ofSize(3));

        PCollection<KV<String, KV<String, Integer>>> withBatchKey = batched.apply(ParDo.of(new FlattenGroupWithSameKey()));

        withBatchKey
                .apply(FileIO.<String, KV<String, KV<String, Integer>>>writeDynamic()
                        .by(KV::getKey)
                        .via(Contextful.fn(kv -> kv.getValue().toString()), TextIO.sink())
                        .withDestinationCoder(StringUtf8Coder.of())
                        .withNaming(subPath -> new MyFileNaming(subPath, ".txt"))
                        .to(TEMP_DIR)
                        .withTempDirectory(OUTPUT_DIR)
                        .withNumShards(1));

        pipeline.run().waitUntilFinish();

    }

    private static final Logger LOGGER = LoggerFactory.getLogger(GroupIntoBatchesWriteTest.class);

    private static class FlattenGroupWithSameKey extends DoFn<KV<String, Iterable<Integer>>, KV<String, KV<String, Integer>>> {

        @ProcessElement
        public void process(@Element KV<String, Iterable<Integer>> element, OutputReceiver<KV<String, KV<String, Integer>>> receiver, @Timestamp Instant ts, BoundedWindow w, PaneInfo p) {
            String key = Y_M_D_H_M_FORMATTER.print(ts.getMillis()) + "/" + UUID.randomUUID();
            for (Integer i : element.getValue()) {
                KV<String, KV<String, Integer>> output = KV.of(key, KV.of(element.getKey(), i));
                LOGGER.info("Processing {}", output);
                receiver.output(output);
            }
        }
    }

    private static final String OUTPUT_DIR = Paths.get("target", GroupIntoBatchesWriteTest.class.getSimpleName(), "output").toAbsolutePath().toString();
    private static final String TEMP_DIR = Paths.get("target", GroupIntoBatchesWriteTest.class.getSimpleName(), "temp").toAbsolutePath().toString();

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

}

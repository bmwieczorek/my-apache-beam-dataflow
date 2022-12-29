package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

public class DynamicShardWindowedTimestampedEventsTest implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicShardWindowedTimestampedEventsTest.class);

    private final Create.TimestampedValues<KV<String, Integer>> timestampedKVEvents = Create.timestamped(
        // W1 [0-5) s
        TimestampedValue.of(KV.of("a", 1), Instant.parse("2021-12-16T00:00:00Z")),
        TimestampedValue.of(KV.of("a", 2), Instant.parse("2021-12-16T00:00:01Z")),
        TimestampedValue.of(KV.of("b", 3), Instant.parse("2021-12-16T00:00:02Z")),

        // W2 [5,10) s
        TimestampedValue.of(KV.of("b", 10), Instant.parse("2021-12-16T00:00:05Z")),
        TimestampedValue.of(KV.of("c", 20), Instant.parse("2021-12-16T00:00:06Z")),
        TimestampedValue.of(KV.of("a", 30), Instant.parse("2021-12-16T00:00:07Z")),
        TimestampedValue.of(KV.of("b", 40), Instant.parse("2021-12-16T00:00:07Z")),
        TimestampedValue.of(KV.of("d", 50), Instant.parse("2021-12-16T00:00:07Z"))
    );

    @Test
    public void shouldCountPerKey() throws IOException {
        // given
        deleteContents(Paths.get("target", DynamicShardWindowedTimestampedEventsTest.class.getSimpleName()).toAbsolutePath());
        String outputDir = Paths.get("target", DynamicShardWindowedTimestampedEventsTest.class.getSimpleName(), "output").toAbsolutePath().toString();
        String tempDir = Paths.get("target", DynamicShardWindowedTimestampedEventsTest.class.getSimpleName(), "temp").toAbsolutePath().toString();

        Pipeline pipeline = Pipeline.create();

        // when
        pipeline.apply(timestampedKVEvents)
            // group per 5 secs window and create kv with key of output2/2021-12-16_00-00 (min granularity)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, String>>(){
                @ProcessElement
                public void process(ProcessContext ctx) {
                    KV<String, Integer> element = ctx.element();
                    if (element == null) return;
                    Instant timestamp = ctx.timestamp();
                    String key = Paths.get(outputDir, timestamp.toString(DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm"))).toString();
                    KV<String, String> kv = KV.of(key, element.getKey() + ":" + element.getValue());
                    LOGGER.info("Processing:{}", kv);
                    ctx.output(kv);
                }

        }))
                    .apply(FileIO
                            .<String, KV<String, String>>writeDynamic()
                            .by(KV::getKey)
                            .via(Contextful.fn(KV::getValue), TextIO.sink())
                            .withDestinationCoder(StringUtf8Coder.of())
                            .withNaming(path -> new MyFileNaming(path, ".txt"))
                            .to(outputDir)
                            .withTempDirectory(tempDir)
                            .withSharding(new PTransform<>() {
                                @Override
                                public PCollectionView<Integer> expand(PCollection<KV<String, String>> input) {
                                    return input
                                            .apply(Combine.globally(Count.<KV<String, String>>combineFn()).withoutDefaults())
                                            .apply(MapElements.via(new SimpleFunction<Long, Integer>() {
                                                @Override
                                                public Integer apply(Long n) {
                                                    int i = (int) (long) n / 2;
                                                    LOGGER.info("numer of shards: {} -> {}", n, i);
                                                    return i;
                                                }
                                            }))
                                            .apply(View.asSingleton());
                                }
                            }));

        pipeline.run().waitUntilFinish();
    }

    static class MyFileNaming implements FileIO.Write.FileNaming {
        private final String path;
        private final String extension;

        public MyFileNaming(String path, String extension) {
            this.path = path;
            this.extension = extension;
        }

        @Override
        public String getFilename(BoundedWindow window, PaneInfo pane, int numShards, int shardIndex, Compression compression) {
            String filename = String.format("%s-currTs-%s-winMaxTs-%s-paneTiming-%s-shard-%s-of-%s%s", path, System.currentTimeMillis(), window.maxTimestamp().toString().replace(":","_").replace(" ","_"), pane.getTiming(), shardIndex, numShards, extension);
            LOGGER.info("Writing data to path='{}'", filename);
            return filename;
        }
    }

    private void deleteContents(Path directoryPath) throws IOException {
        if (Files.exists(directoryPath)) {
            LOGGER.info("Deleting: {}", directoryPath);
            try (Stream<Path> paths = Files.walk(directoryPath)) {
                Stream<File> fileStream = paths.sorted(Comparator.reverseOrder()).map(Path::toFile);
                //noinspection ResultOfMethodCallIgnored
                fileStream.forEach(File::delete);
            }
        }
        Files.createDirectories(directoryPath);
    }
}

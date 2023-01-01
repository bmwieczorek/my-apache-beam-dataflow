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
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.junit.Assert;
import org.junit.Before;
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
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CustomShardingFileIOWriteDynamicTest implements Serializable {

    private static final Create.TimestampedValues<KV<String, Integer>> TIMESTAMPED_KV_EVENTS = Create.timestamped(
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
        Pipeline pipeline = Pipeline.create();

        // when
        pipeline.apply(TIMESTAMPED_KV_EVENTS)
            // group per 5 secs window and create kv with key of output2/2021-12-16_00-00 (min granularity)
            .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))))
            .apply(ParDo.of(new DoFn<KV<String, Integer>, KV<String, String>>(){

                @ProcessElement
                public void process(ProcessContext ctx) {
                    KV<String, Integer> element = ctx.element();
                    if (element == null) return;
                    Instant timestamp = ctx.timestamp();
                    String key = Paths.get(OUTPUT_DIR, timestamp.toString(DateTimeFormat.forPattern("yyyy-MM-dd_HH-mm"))).toString();
                    KV<String, String> kv = KV.of(key, element.getKey() + ":" + element.getValue());
                    LOGGER.info("Processing: {}", kv);
                    ctx.output(kv);
                }

        }))
                    .apply(FileIO
                            .<String, KV<String, String>>writeDynamic()
                            .by(KV::getKey)
                            .via(Contextful.fn(KV::getValue), TextIO.sink())
                            .withDestinationCoder(StringUtf8Coder.of())
                            .withNaming(path -> new MyFileNaming(path, ".txt"))
                            .to(OUTPUT_DIR)
                            .withTempDirectory(TEMP_DIR)
                            .withSharding(new PTransform<>() {
                                @Override
                                public PCollectionView<Integer> expand(PCollection<KV<String, String>> input) {
                                    return input
                                            .apply(Combine.globally(Count.<KV<String, String>>combineFn()).withoutDefaults())
                                            .apply(MapElements.via(new SimpleFunction<Long, Integer>() {
                                                @Override
                                                public Integer apply(Long elementsCountPerWindow) {
                                                    int i = (int) (long) elementsCountPerWindow / 2;
                                                    LOGGER.info("number of shards: {} -> {}", elementsCountPerWindow, i);
                                                    return i;
                                                }
                                            }))
                                            .apply(View.asSingleton());
                                }
                            }));

        pipeline.run().waitUntilFinish();

        // only 1 shard for first window W1 [0-5)
        MatcherAssert.assertThat(
                getLinesInOutputFile("2021-12-16_00-00-winMaxTs-2021-12-16T00_00_04.999Z-paneTiming-ON_TIME-shard-0-of-1.txt"),
                Matchers.containsInAnyOrder("a:1", "a:2", "b:3")
        );

        // 2 shards for second window W2 [5-10)
        List<String> shard1Lines = getLinesInOutputFile("2021-12-16_00-00-winMaxTs-2021-12-16T00_00_09.999Z-paneTiming-ON_TIME-shard-0-of-2.txt");
        Assert.assertTrue(shard1Lines.size() >= 1 && shard1Lines.stream().allMatch(l -> l.endsWith("0")));

        List<String> shard2Lines = getLinesInOutputFile("2021-12-16_00-00-winMaxTs-2021-12-16T00_00_09.999Z-paneTiming-ON_TIME-shard-1-of-2.txt");
        Assert.assertTrue(shard2Lines.size() >= 1 && shard2Lines.stream().allMatch(l -> l.endsWith("0")));
    }

    private static List<String> getLinesInOutputFile(String s) throws IOException {
        try (Stream<String> lines = Files.lines(Path.of(OUTPUT_DIR, s))) {
            return lines.collect(Collectors.toList());
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CustomShardingFileIOWriteDynamicTest.class);

    private static final String OUTPUT_DIR = Paths.get("target", CustomShardingFileIOWriteDynamicTest.class.getSimpleName(), "output").toAbsolutePath().toString();
    private static final String TEMP_DIR = Paths.get("target", CustomShardingFileIOWriteDynamicTest.class.getSimpleName(), "temp").toAbsolutePath().toString();

    @Before
    public void setup() throws IOException {
        deleteContents(OUTPUT_DIR);
        deleteContents(TEMP_DIR);
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
            String filename = String.format("%s-winMaxTs-%s-paneTiming-%s-shard-%s-of-%s%s", path, window.maxTimestamp().toString().replace(":","_").replace(" ","_"), pane.getTiming(), shardIndex, numShards, extension);
            LOGGER.info("Writing data to path='{}'", filename);
            return filename;
        }
    }

    private void deleteContents(String directory) throws IOException {
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

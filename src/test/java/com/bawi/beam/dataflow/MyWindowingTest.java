package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class MyWindowingTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyWindowingTest.class);

    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss'Z'");
    private static final DateTimeFormatter FORMATTER_DASH = DateTimeFormat.forPattern("yyyy-MM-dd-HH-mm-ss.SSS");

    private static final String INPUT = "target/" + MyWindowingTest.class.getSimpleName() + "/input";
    private static final String OUTPUT = "target/" + MyWindowingTest.class.getSimpleName() + "/output";
    private static final String TEMP = "target/" + MyWindowingTest.class.getSimpleName() + "/temp";

    @Before
    public void setup() throws IOException {
        deleteContents(INPUT);
        deleteContents(OUTPUT);
        deleteContents(TEMP);
    }

    @Test
    public void testWindowing() {
        DateTime now = DateTime.now().withSecondOfMinute(0).withZone(DateTimeZone.UTC);
//        DateTime now = DateTime.now().plusHours(1).withSecondOfMinute(0).withZone(DateTimeZone.UTC);

        ArrayBlockingQueue<List<KV<String, String>>> queue = new ArrayBlockingQueue<>(4, true, List.of(
//                List.of(KV.of("a", "2022-10-19T06:00:00Z")),
//                List.of(KV.of("b", "2022-10-19T06:00:11Z")),
//                List.of(KV.of("c", "2022-10-19T06:00:22Z"), KV.of("c", "2022-10-19T06:00:21Z"), KV.of("c", "2022-10-19T06:00:23Z"), KV.of("c", "2022-10-19T06:00:24Z")),
//                List.of(KV.of("l", "2022-10-19T06:00:01Z")) // late

                List.of(KV.of("a", FORMATTER.print(now))),
                List.of(KV.of("b", FORMATTER.print(now.plusSeconds(11)))),
                List.of(KV.of("c", FORMATTER.print(now.plusSeconds(22))), KV.of("c", FORMATTER.print(now.plusSeconds(21))), KV.of("c", FORMATTER.print(now.plusSeconds(23))), KV.of("c", FORMATTER.print(now.plusSeconds(24)))),
                List.of(KV.of("l", FORMATTER.print(now.plusSeconds(1))))
        ));

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1);
        executor.scheduleAtFixedRate(() -> writeListKVElementsToFile(queue),  10 , 10, TimeUnit.SECONDS);

        Pipeline pipeline = Pipeline.create();

        pipeline.apply(TextIO.read().from(INPUT + "/*.log")
                    .watchForNewFiles(
                        Duration.standardSeconds(1),
                        Watch.Growth.afterTotalOf(Duration.standardSeconds(60))
                    )
                )
                .apply(ParDo.of(new LogElementWithWindowDetails<>("1")))

                .apply(WithTimestamps.of(MyWindowingTest::extractAndParseDateTime)
                        .withAllowedTimestampSkew(Duration.standardDays(2))
                )

                .apply(ParDo.of(new LogElementWithWindowDetails<>("2")))

                .apply(Window.<String>into(FixedWindows.of(Duration.standardSeconds(10)))
                        .withAllowedLateness(Duration.standardDays(2))
//                        .triggering(Repeatedly.forever(AfterFirst.of(
//                                AfterPane.elementCountAtLeast(2),
//                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(3)))))
                        .discardingFiredPanes())

                .apply(ParDo.of(new LogElementWithWindowDetails<>("3")))
                .apply(ParDo.of(new ParseToKV()))
                .apply(ParDo.of(new LogElementWithWindowDetails<>("4")))

                .apply(Combine.perKey((Iterable<String> iter) -> StreamSupport.stream(iter.spliterator(), false).collect(Collectors.joining(","))))
                .apply(ParDo.of(new LogElementWithWindowDetails<>("5")))
                .apply(MyConsoleIO.write());

        pipeline.run().waitUntilFinish();

    }

// 10sec IntervalWindow
//11:15:24,300 Written    'a1-10,2021-12-27T09:00:00Z' to target/input/2021-12-27-09-00-00.000.log
//11:15:25,520 Processing 'a1-10,2021-12-27T09:00:00Z',ts=09:00:00Z,wmts=09:00:09.999Z,wsts=09:00:00.000Z,wets=09:00:10.000Z,EARLY

//11:15:34,266 Written    'b2-9,2021-12-27T09:00:11Z' to target/input/2021-12-27-09-00-11.000.log
//11:15:35,326 Processing 'b2-9,2021-12-27T09:00:11Z',ts=09:00:11Z,wmts=09:00:19.999Z,wsts=09:00:10.000Z,wets=09:00:20.000Z,EARLY

//11:15:44,269 Written    'c3-11,2021-12-27T09:00:22Z' to target/input/2021-12-27-09-00-22.000.log
//11:15:44,269 Written    'd3-10,2021-12-27T09:00:21Z' to target/input/2021-12-27-09-00-21.000.log
//11:15:44,270 Written    'e3-8,2021-12-27T09:00:23Z' to target/input/2021-12-27-09-00-23.000.log
//11:15:44,270 Written    'f3-12,2021-12-27T09:00:24Z' to target/input/2021-12-27-09-00-24.000.log
//11:15:44,354 Processing 'f3-12,2021-12-27T09:00:24Z',ts=09:00:24Z,wmts=09:00:29.999Z,wsts=09:00:20.000Z,wets=09:00:30.000Z,EARLY
//11:15:44,354 Processing 'e3-8,2021-12-27T09:00:23Z',ts=09:00:23Z,wmts=09:00:29.999Z,wsts=09:00:20.000Z,wets=09:00:30.000Z,EARLY
//11:15:44,354 Processing 'd3-10,2021-12-27T09:00:21Z',ts=09:00:21Z,wmts=09:00:29.999Z,wsts=09:00:20.000Z,wets=09:00:30.000Z,EARLY
//11:15:44,354 Processing 'c3-11,2021-12-27T09:00:22Z',ts=09:00:22Z,wmts=09:00:29.999Z,wsts=09:00:20.000Z,wets=09:00:30.000Z,EARLY

//11:15:54,267 Written    'l1-10,2021-12-27T09:00:01Z' to target/input/2021-12-27-09-00-01.000.log
//11:15:54,358 Processing 'l1-10,2021-12-27T09:00:01Z',ts=09:00:01Z,wmts=09:00:09.999Z,wsts=09:00:00.000Z,wets=09:00:10.000Z,EARLY



    private static Instant extractAndParseDateTime(String logLine) {
        String dateTime = logLine.substring(logLine.indexOf(",") + 1);
        return Instant.parse(dateTime);
    }

    private static void writeListKVElementsToFile(ArrayBlockingQueue<List<KV<String, String>>> queue) {
        try {
            List<KV<String, String>> kvList = queue.take();
            kvList.forEach(kv -> {
                Instant eventTs = Instant.parse(kv.getValue());
                Path path = Paths.get(INPUT + "/" + FORMATTER_DASH.print(eventTs) + ".log");
                String data = kv.getKey() + "," + kv.getValue();
                try {
                    Files.write(path, data.getBytes());
                } catch (IOException e) {
                    e.printStackTrace();
                }
                LOGGER.info("Written    '" + data + "' to " + path);
            });
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static class ParseToKV extends DoFn<String, KV<String, String>> {
//        @Override
//        public @UnknownKeyFor @NonNull @Initialized Duration getAllowedTimestampSkew() {
//            return Duration.millis(BoundedWindow.TIMESTAMP_MAX_VALUE.getMillis());
//        }

        @ProcessElement
        public void process(@Element String logLine, OutputReceiver<KV<String, String>> outputReceiver) {
            int idx = logLine.indexOf(",");
            String key = logLine.substring(0, idx);
            String dateTime = logLine.substring(idx + 1);
//            outputReceiver.outputWithTimestamp(KV.of(key, dateTime), Instant.parse(dateTime));
            outputReceiver.output(KV.of(key, dateTime));
        }
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

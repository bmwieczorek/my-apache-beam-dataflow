package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.state.BagState;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MyBatchRequestSenderTest implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(MyBatchRequestSenderTest.class);

    private static class MyService {
        private static final Map<String, List<String>> MAPPING = Map.of("A", List.of("_A_", "_A_A_"), "B", List.of("_B_"));
        public static Map<String, List<String>> calculateMapping(List<String> keys) {
            return keys.stream().map(key -> KV.of(key, MAPPING.getOrDefault(key, Collections.emptyList()))).collect(Collectors.toMap(KV::getKey, KV::getValue));
        }
    }

    @Test
    public void shouldSendRequestsInBatches() {
        List<KV<String, Event>> inputEvents = List.of(
                KV.of("a", new Event("A")),
                KV.of("b", new Event("B")),
                KV.of("a", new Event("AA")),
                KV.of("b", new Event("BB")),
                KV.of("a", new Event("AAA")),
                KV.of("b", new Event("BBB")),
                KV.of("a", new Event("AAAA"))
        );

        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, Event>> inputPCollection = pipeline.apply(Create.of(inputEvents));

        PCollection<KV<String, Event>> resultPCollection = inputPCollection
                .apply(ParDo.of(new DoFn<KV<String, Event>, KV<String, Event>>() {
                    @StateId("counter")
                    private final StateSpec<ValueState<Integer>> counterSpec = StateSpecs.value();
                    @StateId("buffer")
                    private final StateSpec<BagState<KV<String, Event>>> bufferSpec = StateSpecs.bag();

                    @ProcessElement
                    public void process(@Element KV<String, Event> kv,
                                        @StateId("counter") ValueState<Integer> counterState,
                                        @StateId("buffer") BagState<KV<String, Event>> bufferState,
                                        OutputReceiver<KV<String, Event>> outputReceiver
                    ) {
                        int cnt = counterState.read() == null ? 0 : counterState.read();
                        cnt = cnt + 1;
                        LOGGER.info("processing {} with {}", kv, cnt);
                        counterState.write(cnt);
                        bufferState.add(kv);
                        if (cnt >= 2) {
                            List<KV<String, Event>> elements = iterableToList(bufferState.read());
                            LOGGER.info("querying: {}", elements);
                            List<String> eventNames = elements.stream().map(_kv -> _kv.getValue().getName()).collect(Collectors.toList());
                            Map<String, List<String>> mapped = MyService.calculateMapping(eventNames);
                            List<KV<String, Event>> mappedElements = enrich(elements, mapped);
                            mappedElements.forEach(e -> {
                                LOGGER.info("outputting: {}", e);
                                outputReceiver.output(e);
                            });

                            counterState.clear();
                            bufferState.clear();
                        }
                    }
                }));

        PAssert.that(resultPCollection).satisfies(resultEventsIter -> {
            List<KV<String, Event>> resultEvents = iterableToList(resultEventsIter);
            Assert.assertEquals(inputEvents.size() - 1, resultEvents.size()); // one b event is skipped
            Assert.assertTrue(resultEvents.contains(KV.of("a", new Event("A:_A_A_"))));
            Assert.assertTrue(resultEvents.contains(KV.of("a", new Event("AAAA:UNKNOWN"))));
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    private static List<KV<String, Event>> iterableToList(Iterable<KV<String, Event>> read) {
        return StreamSupport.stream(read.spliterator(), false).collect(Collectors.toList());
    }

    private static List<KV<String, Event>> enrich(List<KV<String, Event>> elements, Map<String, List<String>> mapped) {
        return elements.stream().map(e -> {
            String name = e.getValue().getName();
            List<String> strings = mapped.get(name);
            String last = strings.size() == 0 ? "UNKNOWN" : strings.get(strings.size() - 1);
            return KV.of(e.getKey(), new Event(name + ":" + last));
        }).collect(Collectors.toList());
    }

    static class Event implements Serializable {
        private final String name;
        public Event(String name) {this.name = name;}
        public String getName() {return name;}
        @Override public String toString() {return "Event{name='" + name + '\'' + '}';}
        @Override public boolean equals(Object o) {if (this == o) return true;if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;return Objects.equals(name, event.name);}
        @Override public int hashCode() {return Objects.hash(name);}
    }
}

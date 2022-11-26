package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MySerializableCoderTest implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySerializableCoderTest.class);

    @Test
    public void shouldSerializeUsingSerializableCoder() {
        Pipeline pipeline = Pipeline.create();
        List<Event> events = List.of(new Event("A"));
        int eventsCount = events.size();

        PCollection<Event> inputEvents = pipeline.apply(Create.of(events));
        PCollection<String> pCollection = inputEvents
                .apply(MapElements.into(TypeDescriptors.strings()).via(e -> {
                    LOGGER.info(e.getName());
                    return e.getName();
                }));

        Assert.assertEquals(SerializableCoder.class, inputEvents.getCoder().getClass());
        PAssert.that(pCollection).satisfies(iterable -> {
            List<String> eventNames = toList(iterable);
            Assert.assertEquals(eventsCount, eventNames.size());
            eventNames.forEach(name -> Assert.assertTrue(name.contains("A")));
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    private static List<String> toList(Iterable<String> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    static class Event implements Serializable { // uses SerializableCoder
        private final String name;
        public Event(String name) {this.name = name;}
        public String getName() {return name;}
        @Override public String toString() {return "Event{name='" + name + '\'' + '}';}
        @Override public boolean equals(Object o) {if (this == o) return true;if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;return Objects.equals(name, event.name);}
        @Override public int hashCode() {return Objects.hash(name);}
    }
}

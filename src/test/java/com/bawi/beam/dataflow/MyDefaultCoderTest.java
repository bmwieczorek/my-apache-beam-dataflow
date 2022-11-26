package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.*;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class MyDefaultCoderTest implements Serializable {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyDefaultCoderTest.class);
    private static final String ENCODED_SUFFIX = "-encoded";
    private static final String DECODED_SUFFIX = "-decoded";

    @Test
    public void shouldSerializeUsingDefaultCustomCoder() {
        Pipeline pipeline = Pipeline.create();
        List<Event> events = List.of(new Event("A"));
        int eventsCount = events.size();

        PCollection<Event> inputEvents = pipeline.apply(Create.of(events));
        PCollection<String> pCollection = inputEvents
                .apply(MapElements.into(TypeDescriptors.strings()).via(e -> {
                    LOGGER.info(e.getName());
                    return e.getName();
                }));

        Assert.assertEquals(EventCoder.class, inputEvents.getCoder().getClass());
        PAssert.that(pCollection).satisfies(iterable -> {
            List<String> eventNames = toList(iterable);
            Assert.assertEquals(eventsCount, eventNames.size());
            eventNames.forEach(name -> Assert.assertTrue(name.contains(ENCODED_SUFFIX) && name.contains(DECODED_SUFFIX)));
            return null;
        });
        pipeline.run().waitUntilFinish();
    }

    private static List<String> toList(Iterable<String> iterable) {
        return StreamSupport.stream(iterable.spliterator(), false).collect(Collectors.toList());
    }

    @DefaultCoder(EventCoder.class) // @DefaultCoder requires coder to implement public static CoderProvider getCoderProvider(
    static class Event {
        private final String name;
        public Event(String name) {this.name = name;}
        public String getName() {return name;}
        @Override public String toString() {return "Event{name='" + name + '\'' + '}';}
        @Override public boolean equals(Object o) {if (this == o) return true;if (o == null || getClass() != o.getClass()) return false;
            Event event = (Event) o;return Objects.equals(name, event.name);}
        @Override public int hashCode() {return Objects.hash(name);}
    }

    public static class EventCoder extends CustomCoder<Event> { // needs to be public static class to be loaded by reflection
        private static final EventCoder INSTANCE = new EventCoder();

        public static EventCoder of() {
            return INSTANCE;
        }

        @Override
        public void encode(Event event, OutputStream outStream) throws IOException {
            String eventName = event.getName() + "-encoded";
            StringUtf8Coder.of().encode(eventName, outStream);
//            String name = eventName + "-encoded";
//            byte[] bytes = name.getBytes();
//            outStream.write(bytes);
        }

        @Override
        public Event decode(InputStream inStream) throws IOException {
//            byte[] bytes = IOUtils.toByteArray(inStream);
//            String eventName = new String(bytes);
            String eventName = StringUtf8Coder.of().decode(inStream);
            return new Event(eventName + "-decoded");
        }

        @SuppressWarnings("unused") // used by CoderRegistry for classes annotated by @DefaultCoder
        public static CoderProvider getCoderProvider() {
            return new CoderProvider() {
                @Override
                public <T> Coder<T> coderFor(TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {
                    if (Event.class.isAssignableFrom(typeDescriptor.getRawType())) {
                        @SuppressWarnings("unchecked")
                        Coder<T> coder = (Coder<T>) EventCoder.of();
                        return coder;
                    }
                    throw new CannotProvideCoderException("Cannot provide EventCoder because " + typeDescriptor + " is not Event");
                }
            };
        }
    }
}

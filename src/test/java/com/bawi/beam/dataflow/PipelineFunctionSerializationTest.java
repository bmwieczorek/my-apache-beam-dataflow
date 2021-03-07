package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.NotSerializableException;

import static org.hamcrest.Matchers.isA;

public class PipelineFunctionSerializationTest {

    static class A { }

    static A staticNonSerializable = new A();

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldPassAsStaticFieldsAreNotSerialized() {
        Pipeline pipeline = Pipeline.create();

        PCollection<String> result = pipeline.apply(Create.of("a", "c"))
                .apply(MapElements.into(TypeDescriptors.strings()).via(s -> {
                    System.out.println(staticNonSerializable);
                    return s;
                }));
        PAssert.that(result).containsInAnyOrder("a", "c");
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void throwNotSerializableWhenNonStaticNonSerializableInstancePassedToMap() {
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectCause(isA(NotSerializableException.class));

        A nonStaticNonSerializable = new A();
        Pipeline pipeline = Pipeline.create();

        pipeline.apply(Create.of("nonStaticNonSerializable", "c"))
                .apply(MapElements.into(TypeDescriptors.strings()).via(s -> {
                    System.out.println(nonStaticNonSerializable);
                    return s;
                }));
        pipeline.run().waitUntilFinish();
    }

    @Test
    public void shouldPassWhenSetupMethodInternallyCalled() {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> result = pipeline.apply(Create.of("a", "c"))
                .apply(ParDo.of(new MyDoFn()));
        PAssert.that(result).containsInAnyOrder("a:A", "c:A");
        pipeline.run().waitUntilFinish();
    }


    @Test
    public void throwNotSerializableWhenNonStaticNonSerializableInstancePassedToDoFn() {
        Throwable cause = Assert.assertThrows(IllegalArgumentException.class, () -> {

            A nonStaticNonSerializable = new A();
            Pipeline pipeline = Pipeline.create();
            pipeline.apply(Create.of("a", "c"))
                    .apply(ParDo.of(new MyDoFn(nonStaticNonSerializable)));
            pipeline.run().waitUntilFinish();

        }).getCause();
        Assert.assertEquals("java.io.NotSerializableException: com.bawi.beam.dataflow.PipelineFunctionSerializationTest$A",
                cause.toString());
    }

    static class MyDoFn extends DoFn<String, String> {
        private A a;
        MyDoFn() {}
        public MyDoFn(A a) {
            this.a = a;
        }
        @Setup
        public void setup() {
            a = new A();
        }

        @ProcessElement
        public void process(@Element String element, OutputReceiver<String> outputReceiver) {
            outputReceiver.output(element + ":" + a.getClass().getSimpleName());
        }
    }
}

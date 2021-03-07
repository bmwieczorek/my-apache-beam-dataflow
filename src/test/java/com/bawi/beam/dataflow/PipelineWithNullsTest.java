package com.bawi.beam.dataflow;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Test;

import java.util.Objects;

public class PipelineWithNullsTest {

    @Test
    public void shouldFilterOutNulls() {
        Pipeline pipeline = Pipeline.create();

        @SuppressWarnings("all")
        PCollection<String> result = pipeline.apply(Create.of("a", null, "c")
                                                .withCoder(NullableCoder.of(StringUtf8Coder.of())))
                                             .apply(Filter.by(Objects::nonNull));
        PAssert.that(result).containsInAnyOrder("a", "c");
        pipeline.run().waitUntilFinish();
    }
}

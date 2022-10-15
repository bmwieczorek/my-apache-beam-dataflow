package com.bawi.beam.dataflow;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;

public class PGPFileDecryptionJobTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() {
        PCollection<String> pCollection = pipeline.apply(new PGPDecryptJob.PGPDecryptPTransform("hello-pp.txt.gpg", System.getenv("GCP_PROJECT"),
                "bartosz-private-key", "bartosz-private-key-passphrase"));

        // assert
        PAssert.thatSingleton(pCollection).isEqualTo("hello-pp.txt.gpg,hello world\n");
        pipeline.run().waitUntilFinish();
    }
}

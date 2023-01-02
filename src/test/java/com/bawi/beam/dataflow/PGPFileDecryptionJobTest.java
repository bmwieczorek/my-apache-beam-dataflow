package com.bawi.beam.dataflow;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;

import java.io.Serializable;
import java.nio.file.Paths;

public class PGPFileDecryptionJobTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() {
        Assume.assumeTrue("Requires sandbox gcp project", System.getenv("GCP_PROJECT").contains("-01-"));
        String filePattern = Paths.get("src", "test", "resources", "hello-pp.txt.gpg").toString();
        PCollection<String> pCollection = pipeline.apply(new PGPDecryptJob.PGPDecryptPTransform(filePattern, System.getenv("GCP_PROJECT"),
                "bartosz-private-key", "bartosz-private-key-passphrase"));

        // assert
        PAssert.thatSingleton(pCollection).isEqualTo("hello-pp.txt.gpg,hello world\n");
        pipeline.run().waitUntilFinish();
    }
}

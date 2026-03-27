package com.bawi.beam.dataflow;

import com.bawi.io.SplitInputStream;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import static com.bawi.io.GzipUtils.gunzip;
import static com.bawi.io.GzipUtils.gzip;

public class GzipDecompressionOnTheFlyJobTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void shouldDecompressOnTheFlyAndImmediatelyEmitElement() throws IOException {
        gzipDecompress(new MyGzipOnTheFlyDecompressor());
    }

    @Test
    public void shouldFirstFullyDecompressThenEmitElement() throws IOException {
        gzipDecompress(new MyGzipDecompressor());
    }

    private void gzipDecompress(DoFn<byte[], String> fn) throws IOException {
        String xml1 = "<ABC><D e=\"1\"/></ABC>";
        String xml2 = "<ABC><D e=\"2\"/></ABC>";
        String xml3 = "<ABC><D e=\"3\"/></ABC>";
        String xml4 = "<ABC><D e=\"4\"/></ABC>";

        PCollection<String> pCollection = pipeline.apply(Create.of(gzip(xml1 + xml2), gzip(xml3 + xml4)))
                .apply(ParDo.of(fn));

        PAssert.that(pCollection).containsInAnyOrder(xml1, xml2, xml3, xml4);
        pipeline.run().waitUntilFinish();
    }

    private static class MyGzipOnTheFlyDecompressor extends DoFn<byte[], String> {
        @ProcessElement
        public void process(@Element byte[] gzippedBytes, OutputReceiver<String> receiver) throws Exception {
            try (InputStream is = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(gzippedBytes)));
                SplitInputStream fwdStream = new SplitInputStream(is, "<ABC", "</ABC>")) {
                byte[] splitBytes;
                while ((splitBytes = fwdStream.readXmlChunkAsBytes()) != null) {
                    String xml = new String(splitBytes);
                    receiver.output(xml);
                }
            }
        }
    }

    private static class MyGzipDecompressor extends DoFn<byte[], String> {
        @ProcessElement
        public void process(@Element byte[] gzippedBytes, OutputReceiver<String> receiver) throws Exception {
            byte[] decompressedBytes = gunzip(gzippedBytes);
            try (InputStream is = new BufferedInputStream(new ByteArrayInputStream(decompressedBytes));
                 SplitInputStream fwdStream = new SplitInputStream(is, "<ABC", "</ABC>")) {
                byte[] splitBytes;
                while ((splitBytes = fwdStream.readXmlChunkAsBytes()) != null) {
                    String xml = new String(splitBytes);
                    receiver.output(xml);
                }
            }
        }
    }

}

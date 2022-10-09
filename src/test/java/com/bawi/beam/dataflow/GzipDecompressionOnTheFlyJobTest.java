package com.bawi.beam.dataflow;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.commons.compress.utils.IOUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

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
                while ((splitBytes = fwdStream.readFwdAsBytes()) != null) {
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
                while ((splitBytes = fwdStream.readFwdAsBytes()) != null) {
                    String xml = new String(splitBytes);
                    receiver.output(xml);
                }
            }
        }
    }


    private byte[] gzip(String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(bytes.length);
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteStream)) {
            gzipOutputStream.write(bytes);
        }
        return byteStream.toByteArray();
    }

    static byte[] gunzip(byte[] compressedPayload) throws IOException {
        try (
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedPayload);
                GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream)
        ) {
            return IOUtils.toByteArray(gzipInputStream);
        }
    }

    private static class SplitInputStream implements AutoCloseable {

        byte[] startTag;

        byte[] endTag;

        private final InputStream is;

        private static final int MAX_BYTES_READ = 1024 * 1024;

        public SplitInputStream(InputStream is, String startTag, String endTag) {
            this.is = is;
            this.startTag = startTag.getBytes(StandardCharsets.UTF_8);
            this.endTag = endTag.getBytes(StandardCharsets.UTF_8);
        }

        public byte[] readFwdAsBytes() throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream(1024 * 128);
            if (readUntilMatch(startTag, out, true)) {
                out.write(startTag);
                if (readUntilMatch(endTag, out, false)) {
                    return out.toByteArray();
                }
            }
            return null;
        }

        private boolean readUntilMatch(byte[] needle, ByteArrayOutputStream dataRead, boolean skipData) throws IOException {
            int i = 0;
            int pos = -1;
            while (true) {
                pos++;
                int b = is.read();
                // end of file:
                if (b == -1) {
                    return false;
                }

                // skip or save to buffer:
                if (!skipData) {
                    dataRead.write(b);
                }

                // check if we're matching:
                if (b == needle[i]) {
                    i++;
                    if (i >= needle.length) {
                        return true;
                    }
                } else {
                    i = 0;
                }

                // TODO: verify if condition: pos >= MAX_BYTES_READ is necessary
                // see if we've passed the stop point:
                if (skipData && i == 0 && pos >= MAX_BYTES_READ) {
                    return false;
                }
            }
        }

        @Override
        public void close() throws Exception {
            is.close();
        }
    }
}

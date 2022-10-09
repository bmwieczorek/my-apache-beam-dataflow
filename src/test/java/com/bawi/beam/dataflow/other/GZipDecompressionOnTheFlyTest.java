package com.bawi.beam.dataflow.other;

import org.junit.Test;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;

public class GZipDecompressionOnTheFlyTest {
    @Test
    public void testDecompression() throws Exception {
        // given
        List<String> receivedMessages = new ArrayList<>();

        String multiRootElementXml = "<ABC><D e=\"123\"/></ABC><ABC><D e=\"456\"/></ABC>";
        byte[] gzippedBytes = gzipCompress(multiRootElementXml);
        Consumer<String> messageReceiver = message -> receivedMessages.add("Received: " + message);

        // when
        try (InputStream is = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(gzippedBytes)));
             SplitInputStream fwdStream = new SplitInputStream(is, "<ABC", "</ABC>")) {
            byte[] splitBytes;
            while ((splitBytes = fwdStream.readFwdAsBytes()) != null) {
                String singleRootXml = new String(splitBytes);
                messageReceiver.accept(singleRootXml);
            }
        }

        // then
        assertEquals(Arrays.asList(
                "Received: <ABC><D e=\"123\"/></ABC>",
                "Received: <ABC><D e=\"456\"/></ABC>"), receivedMessages);

    }

    private byte[] gzipCompress(String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(bytes.length);
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteStream)) {
            gzipOutputStream.write(bytes);
        }
        return byteStream.toByteArray();
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

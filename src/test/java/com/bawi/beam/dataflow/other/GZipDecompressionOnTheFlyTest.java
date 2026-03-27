package com.bawi.beam.dataflow.other;

import com.bawi.io.SplitInputStream;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
import static com.bawi.io.GzipUtils.gzip;
import static org.junit.Assert.assertEquals;

public class GZipDecompressionOnTheFlyTest {
    @Test
    public void testDecompression() throws Exception {
        // given
        List<String> receivedMessages = new ArrayList<>();

        String multiRootElementXml = "<ABC><D e=\"123\"/></ABC><ABC><D e=\"456\"/></ABC>";
        byte[] gzippedBytes = gzip(multiRootElementXml);
        Consumer<String> messageReceiver = message -> receivedMessages.add("Received: " + message);

        // when
        try (InputStream is = new BufferedInputStream(new GZIPInputStream(new ByteArrayInputStream(gzippedBytes)));
            SplitInputStream fwdStream = new SplitInputStream(is, "<ABC", "</ABC>")) {
            byte[] splitBytes;
            while ((splitBytes = fwdStream.readXmlChunkAsBytes()) != null) {
                String singleRootXml = new String(splitBytes);
                messageReceiver.accept(singleRootXml);
            }
        }

        // then
        assertEquals(Arrays.asList(
                "Received: <ABC><D e=\"123\"/></ABC>",
                "Received: <ABC><D e=\"456\"/></ABC>"), receivedMessages);

    }

}

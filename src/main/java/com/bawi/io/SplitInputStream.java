package com.bawi.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class SplitInputStream implements AutoCloseable {
    private static final int MAX_BYTES_READ = 1024 * 1024;

    private final InputStream is;
    byte[] startTag;
    byte[] endTag;

    public SplitInputStream(InputStream is, String startTag, String endTag) {
        this.is = is;
        this.startTag = startTag.getBytes(StandardCharsets.UTF_8);
        this.endTag = endTag.getBytes(StandardCharsets.UTF_8);
    }

    public byte[] readXmlChunkAsBytes() throws IOException {
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

package com.bawi.io;

import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class GzipUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(GzipUtils.class);

    public static byte[] gzip(String s) throws IOException {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(bytes.length);
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(byteStream)) {
            gzipOutputStream.write(bytes);
        }
        return byteStream.toByteArray();
    }

    public static byte[] gunzip(byte[] compressedPayload) {
        try (
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(compressedPayload);
                GZIPInputStream gzipInputStream = new GZIPInputStream(byteArrayInputStream)
        ) {
            return IOUtils.toByteArray(gzipInputStream);
        } catch (IOException e) {
            LOGGER.error("Failed to gunzip", e);
            throw new RuntimeException(e);
        }
    }
}

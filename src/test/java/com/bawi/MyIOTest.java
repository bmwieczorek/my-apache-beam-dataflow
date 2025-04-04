package com.bawi;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class MyIOTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MyIOTest.class);

    @Test
    public void test() throws IOException {
        // given
        LOGGER.info("my.system.property={}", System.getProperty("my.system.property"));
        LOGGER.info("my.env.variable={}", System.getenv("my.env.variable"));
        String text = "Hello World";
        String fileRelativePath = "target/myFile.txt";

        // when
        Path path = Path.of(fileRelativePath);
        Files.write(path, text.getBytes());
        String actual = new String(Files.readAllBytes(path));

        // then
        LOGGER.info("text={}", actual);
        Assert.assertEquals(text, actual);
    }
}

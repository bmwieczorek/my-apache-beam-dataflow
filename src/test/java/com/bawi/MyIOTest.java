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
        LOGGER.info("my.system.property={}", System.getProperty("my.system.property"));
        LOGGER.info("my.env.variable={}", System.getenv("my.env.variable"));
        String text = "Hello World";
        String fileRelativePath = "target/myFile.txt";
        Files.write(Path.of(fileRelativePath), text.getBytes());
        String actual = new String(Files.readAllBytes(Path.of(fileRelativePath)));
        Assert.assertEquals(text, actual);
    }
}

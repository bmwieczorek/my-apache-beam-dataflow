package com.bawi.beam.dataflow;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.io.FilenameUtils;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class ReadTextFileTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() throws IOException {
        // given - generate file
        Files.write(Paths.get("target/myFile.txt"), "abc123".getBytes(), StandardOpenOption.CREATE);

        // when - read file
        PCollection<String> pCollection = pipeline.apply(FileIO.match().filepattern("target/my*.txt"))
                .apply(FileIO.readMatches())
                .apply("Read to file name and bytes", MapElements
                        .into(kvs(strings(), TypeDescriptor.of(byte[].class)))
                        .via(readableFile -> {
                            String resourceId = readableFile.getMetadata().resourceId().toString();
                            try {
                                String s = readableFile.readFullyAsUTF8String();
                                return KV.of(resourceId, readableFile.readFullyAsBytes());
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to read the file " + resourceId, e);
                            }
                        })
                ).apply("Convert to String", MapElements.into(strings()).via(fileNameAndBytes -> {
                    String fileName = FilenameUtils.getName(fileNameAndBytes.getKey());
                    return fileName + "," + new String(fileNameAndBytes.getValue());
                }));

        // assert
        PAssert.thatSingleton(pCollection).isEqualTo("myFile.txt,abc123");
        pipeline.run().waitUntilFinish();
    }
}

package com.bawi.beam.dataflow;

import com.google.auto.service.AutoService;
import org.apache.beam.sdk.harness.JvmInitializer;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.options.PipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

@AutoService(JvmInitializer.class)
public class KerberosJvmInitializer implements JvmInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(KerberosJvmInitializer.class);

    // used by dataflow workers to copy kafka kerberos files from gcs to workers local fs
    // executed only with non-local DataflowRunner

    @Override
    public void beforeProcessing(PipelineOptions options) {
        MyKerberizedKafkaToPubsub.Options kerberizedOptions = options.as(MyKerberizedKafkaToPubsub.Options.class);

        try {
            copyToLocal(kerberizedOptions.getGcsKeyTabPath(), kerberizedOptions.getLocalKeyTabPath());
            copyToLocal(kerberizedOptions.getGcsKrb5Path(), kerberizedOptions.getLocalKrb5Path());
            copyToLocal(kerberizedOptions.getGcsTrustStorePath(), kerberizedOptions.getLocalTrustStorePath());
            System.setProperty("java.security.krb5.conf", kerberizedOptions.getLocalKrb5Path());
        } catch (IOException e) {
            LOGGER.error("Unable to copy from GCS to local");
            throw new RuntimeException(e);
        }
    }

    private void copyToLocal(String inputPath, String outputPath) throws IOException {
        LOGGER.info("Copying {} to {}", inputPath, outputPath);
        File outputFile = new File(outputPath);
        //noinspection ResultOfMethodCallIgnored
        outputFile.getParentFile().mkdirs();
        try (ReadableByteChannel inChannel = FileSystems.open(FileSystems.matchNewResource(inputPath, false));
             FileOutputStream fileOutputStream = new FileOutputStream(outputFile)) {
            fileOutputStream.getChannel().transferFrom(inChannel, 0, Long.MAX_VALUE);
            LOGGER.info("Copied {} to {}", inputPath, outputFile);
        }
    }
}

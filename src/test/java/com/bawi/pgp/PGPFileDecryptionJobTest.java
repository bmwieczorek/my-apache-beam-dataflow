package com.bawi.pgp;

import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.KeyringConfigCallbacks;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.commons.io.FilenameUtils;
import org.bouncycastle.openpgp.PGPException;
import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.security.NoSuchProviderException;
import java.security.Security;

import static com.bawi.pgp.GPGEncryptionDecryptionTest.*;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class PGPFileDecryptionJobTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    static class PGPDecryptFn extends DoFn<KV<String,byte[]>,String> {

        private InMemoryKeyring keyringConfig;

        @Setup
        public void setup() throws IOException, PGPException {
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());

            keyringConfig = KeyringConfigs.forGpgExportedKeys(KeyringConfigCallbacks.withPassword("bartek"));

            keyringConfig.addPublicKey(PUBLIC_KEY.getBytes());
            keyringConfig.addSecretKey(PRIVATE_KEY.getBytes());
        }

        @ProcessElement
        public void process(@Element KV<String,byte[]> kv, OutputReceiver<String> outputReceiver) throws IOException, NoSuchProviderException {
            byte[] decrypted;
            try (ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(kv.getValue());
                 ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream()
            ) {
                decrypt(keyringConfig, encryptedInputStream, decryptedOutputStream);
                decrypted = decryptedOutputStream.toByteArray();
            }
            outputReceiver.output(FilenameUtils.getName(kv.getKey()) + "," + new String(decrypted));
        }
    }

    @Test
    public void test() throws IOException {
        PCollection<String> pCollection = pipeline.apply(FileIO.match().filepattern("hello.txt.gpg"))
                .apply(FileIO.readMatches())
                .apply(MapElements
                        .into(kvs(strings(), TypeDescriptor.of(byte[].class)))
                        .via(readableFile -> {
                            String resourceId = readableFile.getMetadata().resourceId().toString();
                            try {
                                return KV.of(resourceId, readableFile.readFullyAsBytes());
                            } catch (IOException e) {
                                throw new RuntimeException("Failed to read the file " + resourceId, e);
                            }
                        })
                ).apply(ParDo.of(new PGPDecryptFn()));

        // assert
        PAssert.thatSingleton(pCollection).isEqualTo("hello.txt.gpg,hello world\n");
        pipeline.run().waitUntilFinish();
    }
}

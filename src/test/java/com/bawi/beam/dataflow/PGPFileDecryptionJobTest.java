package com.bawi.beam.dataflow;

import com.bawi.pgp.PGPEncryptionUtils;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
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
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.NoSuchProviderException;
import java.util.stream.Stream;

import static com.bawi.pgp.PGPEncryptionUtils.createKeyringConfigFromPublicKeyPrivateKeyAndPassphrase;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class PGPFileDecryptionJobTest implements Serializable {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void test() {
        Assume.assumeTrue("Requires pgp files",
                Stream.of("hello-pp.txt.gpg", "bartek-pp-public.asc", "bartek-pp-private.asc", "bartek-pp.txt")
                        .allMatch(name -> Path.of("gpg", name).toFile().exists()));

        PCollection<String> pCollection = pipeline.apply(FileIO.match().filepattern("gpg/hello-pp.txt.gpg"))
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
        PAssert.thatSingleton(pCollection).isEqualTo("hello-pp.txt.gpg,hello world\n");
        pipeline.run().waitUntilFinish();
    }

    static class PGPDecryptFn extends DoFn<KV<String,byte[]>,String> {

        private KeyringConfig keyringConfig;

        @Setup
        public void setup() throws IOException, PGPException {
            keyringConfig = createKeyringConfigFromPublicKeyPrivateKeyAndPassphrase(
                    Files.readAllBytes(Path.of("gpg", "bartek-pp-public.asc")),
                    Files.readAllBytes(Path.of("gpg", "bartek-pp-private.asc")),
                    Files.readAllBytes(Path.of("gpg", "bartek-pp.txt"))
            );
        }

        @ProcessElement
        public void process(@Element KV<String,byte[]> kv, OutputReceiver<String> outputReceiver) throws IOException, NoSuchProviderException {
            byte[] decrypted = PGPEncryptionUtils.decrypt(kv.getValue(), keyringConfig);
            outputReceiver.output(FilenameUtils.getName(kv.getKey()) + "," + new String(decrypted));
        }
    }
}

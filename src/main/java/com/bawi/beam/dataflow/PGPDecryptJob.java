package com.bawi.beam.dataflow;

import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.bouncycastle.openpgp.PGPException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.security.NoSuchProviderException;

import static com.bawi.beam.dataflow.SecretManagerUtils.accessSecretVersion;
import static com.bawi.pgp.PGPEncryptionUtils.createKeyringConfigFromPrivateKeyAndPassphrase;
import static com.bawi.pgp.PGPEncryptionUtils.decrypt;
import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class PGPDecryptJob {

    public static void main(String[] args) {
        args = DataflowUtils.updateDataflowArgs(args,
                "--filePattern=gs://" + System.getenv("GCP_PROJECT") + "-" + System.getenv("GCP_OWNER") + "/hello-pp.txt.gpg",
                "--projectId=" + System.getenv("GCP_PROJECT"),
                "--privateKeySecretName=bartosz-private-key", "--privateKeyPassphraseSecretName=bartosz-private-key-passphrase");

        Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
        Pipeline pipeline = Pipeline.create(options);

        pipeline.apply(new PGPDecryptPTransform(options.getFilePattern(), options.getProjectId(), options.getPrivateKeySecretName(), options.getPrivateKeyPassphraseSecretName()))
                .apply(MyConsoleIO.write());

        pipeline.run();
    }

    @SuppressWarnings("unused")
    public interface Options extends PipelineOptions {
        @Validation.Required
        String getFilePattern();
        void setFilePattern(String value);

        @Validation.Required
        String getProjectId();
        void setProjectId(String value);

        @Validation.Required
        String getPrivateKeySecretName();
        void setPrivateKeySecretName(String value);

        @Validation.Required
        String getPrivateKeyPassphraseSecretName();
        void setPrivateKeyPassphraseSecretName(String value);
    }

    static class PGPDecryptPTransform extends PTransform<PBegin, PCollection<String>> {

        private final String filePattern;
        private final String project;
        private final String privateKeySecretName;
        private final String privateKeyPassphraseSecretName;

        public PGPDecryptPTransform(String filePattern, String project, String privateKeySecretName, String privateKeyPassphraseSecretName) {
            this.filePattern = filePattern;
            this.project = project;
            this.privateKeySecretName = privateKeySecretName;
            this.privateKeyPassphraseSecretName = privateKeyPassphraseSecretName;
        }

        @Override
        public PCollection<String> expand(PBegin input) {
            return input.getPipeline().apply(FileIO.match().filepattern(filePattern))
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
                    ).apply(ParDo.of(new PGPDecryptFn(project, privateKeySecretName, privateKeyPassphraseSecretName)));
        }
    }

    static class PGPDecryptFn extends DoFn<KV<String,byte[]>,String> {

        private final String project;
        private final String privateKeySecretName;
        private final String privateKeyPassphraseSecretName;
        private KeyringConfig keyringConfig;

        public PGPDecryptFn(String project, String privateKeySecretName, String privateKeyPassphraseSecretName) {
            this.project = project;
            this.privateKeySecretName = privateKeySecretName;
            this.privateKeyPassphraseSecretName = privateKeyPassphraseSecretName;
        }

        @Setup
        public void setup() throws IOException, PGPException {
            byte[] privateKey = accessSecretVersion(project, privateKeySecretName, "2");
            byte[] privateKeyPassphrase = accessSecretVersion(project, privateKeyPassphraseSecretName, "2");
            keyringConfig = createKeyringConfigFromPrivateKeyAndPassphrase(privateKey, privateKeyPassphrase);
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
            outputReceiver.output(new File(kv.getKey()).getName() + "," + new String(decrypted));
        }
    }

}

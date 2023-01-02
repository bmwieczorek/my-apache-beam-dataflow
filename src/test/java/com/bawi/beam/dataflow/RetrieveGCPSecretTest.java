package com.bawi.beam.dataflow;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.util.zip.CRC32C;
import java.util.zip.Checksum;

public class RetrieveGCPSecretTest {

    @Test
    public void test() throws IOException {
        Assume.assumeTrue("Requires sandbox gcp project",
                System.getenv("GCP_PROJECT").contains("-01-"));
        System.out.println(new String(accessSecretVersion(System.getenv("GCP_PROJECT"), "bartosz-private-key", "latest")));
    }

    public static byte[] accessSecretVersion(String project, String secret, String secretVersion) throws IOException {
        try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
            SecretVersionName secretVersionName = SecretVersionName.of(project, secret, secretVersion);
            AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
            byte[] data = response.getPayload().getData().toByteArray();
            Checksum checksum = new CRC32C();
            checksum.update(data, 0, data.length);
            if (response.getPayload().getDataCrc32C() != checksum.getValue()) {
                throw new RuntimeException("Data corruption detected");
            }
            return data;
        }
    }

}

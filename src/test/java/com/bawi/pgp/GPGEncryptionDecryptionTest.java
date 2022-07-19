package com.bawi.pgp;

import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.KeyringConfigCallbacks;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.util.io.Streams;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.SignatureException;

public class GPGEncryptionDecryptionTest {

    static final String PUBLIC_KEY = "-----BEGIN PGP PUBLIC KEY BLOCK-----\n" +
            // ...
            "-----END PGP PUBLIC KEY BLOCK-----";

    static final String PRIVATE_KEY = "-----BEGIN PGP PRIVATE KEY BLOCK-----\n" +
            // ...
            "-----END PGP PRIVATE KEY BLOCK-----";

    @Test
    public void test() throws IOException, PGPException, NoSuchProviderException, SignatureException, NoSuchAlgorithmException {
        // given
        String publicKey="-----BEGIN PGP PUBLIC KEY BLOCK-----\n" +
                // ...
                "-----END PGP PUBLIC KEY BLOCK-----";

        String privateKey = "-----BEGIN PGP PRIVATE KEY BLOCK-----\n" +
                // ...
                "-----END PGP PRIVATE KEY BLOCK-----";

        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
//        InputStream pubStream = new ByteArrayInputStream(publicKey.getBytes());
//        InputStream secStream = new ByteArrayInputStream(privateKey.getBytes());
//        KeyringConfig keyringConfig =
//                KeyringConfigs.withKeyRingsFromStreams(pubStream, secStream, KeyringConfigCallbacks.withPassword("bartek"));

        InMemoryKeyring keyringConfig =
                KeyringConfigs.forGpgExportedKeys(KeyringConfigCallbacks.withPassword("bartek"));

        keyringConfig.addPublicKey(PUBLIC_KEY.getBytes());
        keyringConfig.addSecretKey(PRIVATE_KEY.getBytes());

        String plainText = "hello world";
        byte[] encryptedBytes;
        byte[] decryptedBytes;
        try (
                ByteArrayInputStream plainTextInputStream = new ByteArrayInputStream(plainText.getBytes());
                ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream()
        ) {
            encrypt(keyringConfig, plainTextInputStream, encryptedOutputStream, "bartek@example.com");
            encryptedBytes = encryptedOutputStream.toByteArray();
        }

        try (
                ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytes);
                ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream()
        ) {
            decrypt(keyringConfig, encryptedInputStream, decryptedOutputStream);
            decryptedBytes = decryptedOutputStream.toByteArray();
        }

        Assert.assertEquals(plainText, new String(decryptedBytes));

//        encrypt(keyringConfig, new FileInputStream("a_plain.txt"), new FileOutputStream("a.pgp"), "bartek@example.com");
//        decrypt(keyringConfig, new FileInputStream("a.gpg"), new FileOutputStream("a_decrypted.txt"));
    }

    public static void encrypt(InMemoryKeyring keyringConfig, InputStream inputStream, OutputStream outputStream, String recipient) throws IOException, PGPException, NoSuchAlgorithmException, SignatureException, NoSuchProviderException {
        try (
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

                OutputStream encryptedStream = BouncyGPG
                    .encryptToStream()
                    .withConfig(keyringConfig)
                    .withStrongAlgorithms()
                    .toRecipient(recipient)
                    .andDoNotSign()
                    .binaryOutput()
                    .andWriteTo(bufferedOutputStream)
        ) {
            Streams.pipeAll(inputStream, encryptedStream);
        }
    }

    public static void decrypt(InMemoryKeyring keyringConfig, InputStream encryptedInputStream, OutputStream outputStream)
            throws IOException, NoSuchProviderException {
        try (
                BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

                InputStream decryptedStream = BouncyGPG
                        .decryptAndVerifyStream()
                        .withConfig(keyringConfig)
                        .andIgnoreSignatures()
                        .fromEncryptedInputStream(encryptedInputStream)
        ) {
            Streams.pipeAll(decryptedStream, bufferedOutputStream);
        }
    }
}

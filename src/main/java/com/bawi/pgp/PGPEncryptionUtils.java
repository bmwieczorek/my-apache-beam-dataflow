package com.bawi.pgp;

import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.KeyringConfigCallbacks;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.util.io.Streams;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.SignatureException;

public class PGPEncryptionUtils {

    public static byte[] decrypt(byte[] encryptedBytes, KeyringConfig keyringConfig) throws IOException, NoSuchProviderException {
        byte[] decryptedBytes;
        try (
                ByteArrayInputStream encryptedInputStream = new ByteArrayInputStream(encryptedBytes);
                ByteArrayOutputStream decryptedOutputStream = new ByteArrayOutputStream()
        ) {
            decryptStream(keyringConfig, encryptedInputStream, decryptedOutputStream);
            decryptedBytes = decryptedOutputStream.toByteArray();
        }
        return decryptedBytes;
    }

    public static byte[] encrypt(byte[] plainTextBytes, KeyringConfig keyringConfig, String recipient) throws IOException, PGPException, NoSuchAlgorithmException, SignatureException, NoSuchProviderException {
        byte[] encryptedBytes;
        try (
                ByteArrayInputStream plainTextInputStream = new ByteArrayInputStream(plainTextBytes);
                ByteArrayOutputStream encryptedOutputStream = new ByteArrayOutputStream()
        ) {
            encryptStream(keyringConfig, plainTextInputStream, encryptedOutputStream, recipient);
            encryptedBytes = encryptedOutputStream.toByteArray();
        }
        return encryptedBytes;
    }

    private static void encryptStream(KeyringConfig keyringConfig, InputStream inputStream, OutputStream outputStream, String recipient) throws IOException, PGPException, NoSuchAlgorithmException, SignatureException, NoSuchProviderException {
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

    private static void decryptStream(KeyringConfig keyringConfig, InputStream encryptedInputStream, OutputStream outputStream)
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

    public static KeyringConfig createKeyringConfigFromPrivateKeyAndPassphrase(byte[] privateKey, byte[] privateKeyPassphrase) throws IOException, PGPException {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        InMemoryKeyring keyringConfig = KeyringConfigs.forGpgExportedKeys(KeyringConfigCallbacks.withPassword(new String(privateKeyPassphrase)));
        keyringConfig.addSecretKey(privateKey);
        return keyringConfig;
    }

    public static KeyringConfig createKeyringConfigFromPublicKeyPrivateKeyAndPassphrase(byte[] publicKey, byte[] privateKey, byte[] privateKeyPassphrase) throws IOException, PGPException {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
        InMemoryKeyring keyringConfig = KeyringConfigs.forGpgExportedKeys(KeyringConfigCallbacks.withPassword(new String(privateKeyPassphrase)));
        keyringConfig.addPublicKey(publicKey);
        keyringConfig.addSecretKey(privateKey);
        return keyringConfig;
    }
}

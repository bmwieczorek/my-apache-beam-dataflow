package com.bawi.pgp;

import name.neuhalfen.projects.crypto.bouncycastle.openpgp.BouncyGPG;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.callbacks.KeyringConfigCallbacks;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.InMemoryKeyring;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfigs;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.util.io.Streams;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.security.SignatureException;

public class PGPEncryptionUtils {

    public static void encrypt(KeyringConfig keyringConfig, InputStream inputStream, OutputStream outputStream, String recipient) throws IOException, PGPException, NoSuchAlgorithmException, SignatureException, NoSuchProviderException {
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

    public static void decrypt(KeyringConfig keyringConfig, InputStream encryptedInputStream, OutputStream outputStream)
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

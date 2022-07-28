package com.bawi.pgp;

import name.neuhalfen.projects.crypto.bouncycastle.openpgp.keys.keyrings.KeyringConfig;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

public class GPGEncryptionDecryptionTest {

    @Test
    public void test() throws Exception {
        Assume.assumeTrue("Requires pgp files",
                Stream.of("hello-pp.txt.gpg", "bartek-pp-public.asc", "bartek-pp-private.asc", "bartek-pp.txt")
                        .allMatch(name -> Path.of("gpg", name).toFile().exists()));

        // given
        String plainText = "hello world";
        KeyringConfig keyringConfig = PGPEncryptionUtils.createKeyringConfigFromPublicKeyPrivateKeyAndPassphrase(
                Files.readAllBytes(Path.of("gpg", "bartek-pp-public.asc")),
                Files.readAllBytes(Path.of("gpg", "bartek-pp-private.asc")),
                Files.readAllBytes(Path.of("gpg", "bartek-pp.txt"))
        );

        // when
        byte[] encryptedBytes = PGPEncryptionUtils.encrypt(plainText.getBytes(), keyringConfig, "bartek@example.com");
        byte[] decryptedBytes = PGPEncryptionUtils.decrypt(encryptedBytes, keyringConfig);

        // then
        Assert.assertEquals(plainText, new String(decryptedBytes));

//        encrypt(keyringConfig, new FileInputStream("a_plain.txt"), new FileOutputStream("a.pgp"), "bartek@example.com");
//        decrypt(keyringConfig, new FileInputStream("a.gpg"), new FileOutputStream("a_decrypted.txt"));
    }
}

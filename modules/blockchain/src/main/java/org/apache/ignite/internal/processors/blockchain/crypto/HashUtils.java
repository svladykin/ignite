package org.apache.ignite.internal.processors.blockchain.crypto;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Security;
import java.util.Random;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.IgniteUtils;

import static java.util.Objects.requireNonNull;
import static org.apache.ignite.internal.util.IgniteUtils.newInstance;

public final class HashUtils {
    /** */
    public static final String SHA3_256 = "SHA3-256"; // TODO make configurable

    /** Bouncy castle. */
    public static final String CRYPTO_PROVIDER = "BC"; // TODO make configurable

    /** */
    private static final String CRYPTO_PROVIDER_CLASS = "org.bouncycastle.jce.provider.BouncyCastleProvider"; // TODO make configurable

    static {
        try {
            Security.addProvider(requireNonNull(newInstance(CRYPTO_PROVIDER_CLASS)));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * SHA3-256
     *
     * @param bytes Any byte array to hash.
     * @return 256-bit hash.
     */
    public static byte[] sha3_256(byte[] bytes) {
        requireNonNull(bytes);

        MessageDigest digest;

        try {
            digest = MessageDigest.getInstance(SHA3_256, CRYPTO_PROVIDER);
        }
        catch (NoSuchAlgorithmException | NoSuchProviderException e) {
            throw new IllegalStateException(e);
        }

        return digest.digest(bytes);
    }

    public static String hex(byte[] bytes) {
        return IgniteUtils.byteArray2HexString(bytes);
    }

    public static void main(String[] args) {
        byte[] key = new byte[5];
        new Random().nextBytes(key);
        byte[] hash = sha3_256(key);

        System.out.println(hex(key) + " -> " + hex(hash));
    }
}

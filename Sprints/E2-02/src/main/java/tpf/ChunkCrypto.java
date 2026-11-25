package tpf;

import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HexFormat;

public final class ChunkCrypto {

    public static final int KEY_BYTES = 32;
    public static final int SALT_BYTES = 32;
    public static final int NONCE_BYTES = 12;
    public static final int GCM_TAG_BITS = 128;
    public static final int KEY_VERSION = 1;

    private static final byte[] HKDF_INFO = "tpf-chunk-v1".getBytes(StandardCharsets.UTF_8);
    private static final byte[] AAD_PREFIX = "tpf-v1".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RNG = new SecureRandom();

    private ChunkCrypto() {
    }

    public static byte[] loadOrCreateMasterKey(Path keyPath) throws IOException {
        if (keyPath.getParent() != null) {
            Files.createDirectories(keyPath.getParent());
        }

        if (Files.exists(keyPath)) {
            byte[] key = Files.readAllBytes(keyPath);
            if (key.length != KEY_BYTES) {
                throw new IllegalStateException(
                        "master.key inválida: esperado " + KEY_BYTES + " bytes, encontrado " + key.length + " bytes."
                );
            }
            return key;
        }

        byte[] key = new byte[KEY_BYTES];
        RNG.nextBytes(key);

        Files.write(keyPath, key, StandardOpenOption.CREATE_NEW);

        if (FileSystems.getDefault().supportedFileAttributeViews().contains("posix")) {
            Files.setPosixFilePermissions(keyPath, PosixFilePermissions.fromString("r--------"));
        } else {
            File file = keyPath.toFile();
            file.setReadable(false, false);
            file.setReadable(true, true);
            file.setWritable(false, false);
        }

        return key;
    }

    public static Sealed seal(byte[] plaintextPadded, byte[] masterKey) throws GeneralSecurityException {
        if (masterKey == null || masterKey.length != KEY_BYTES) {
            throw new IllegalArgumentException("masterKey tem de ter 32 bytes.");
        }

        byte[] salt = new byte[SALT_BYTES];
        byte[] nonce = new byte[NONCE_BYTES];
        RNG.nextBytes(salt);
        RNG.nextBytes(nonce);

        byte[] dek = deriveDek(masterKey, salt);
        byte[] aad = buildAad(KEY_VERSION, salt, nonce);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(GCM_TAG_BITS, nonce));
        cipher.updateAAD(aad);
        byte[] ciphertext = cipher.doFinal(plaintextPadded);

        String chunkId = sha256Hex(ciphertext);
        return new Sealed(chunkId, KEY_VERSION, salt, nonce, ciphertext);
    }

    public static byte[] open(Sealed sealed, byte[] masterKey) throws GeneralSecurityException {
        if (masterKey == null || masterKey.length != KEY_BYTES) {
            throw new IllegalArgumentException("masterKey tem de ter 32 bytes.");
        }

        // 1. Verificar hash contra chunkId
        String actualHash = sha256Hex(sealed.ciphertext());
        if (!actualHash.equalsIgnoreCase(sealed.chunkId())) {
            throw new SecurityException("Integridade falhou: hash SHA-256 não corresponde ao chunkId.");
        }

        // 2. Verificar versão da chave
        if (sealed.keyVersion() != KEY_VERSION) {
            throw new IllegalArgumentException("Versão de chave não suportada: " + sealed.keyVersion());
        }

        // 3. Derivar DEK e verificar autenticação GCM
        byte[] dek = deriveDek(masterKey, sealed.salt());
        byte[] aad = buildAad(sealed.keyVersion(), sealed.salt(), sealed.nonce());

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(dek, "AES"), new GCMParameterSpec(GCM_TAG_BITS, sealed.nonce()));
        cipher.updateAAD(aad);
        return cipher.doFinal(sealed.ciphertext());
    }

    public static String formatHeader(Sealed sealed) {
        Base64.Encoder b64 = Base64.getEncoder();
        return sealed.keyVersion() + ";"
                + b64.encodeToString(sealed.salt()) + ";"
                + b64.encodeToString(sealed.nonce());
    }

    public static Sealed parseFromDisk(String chunkId, Path binPath, Path hdrPath) throws IOException {
        byte[] ciphertext = Files.readAllBytes(binPath);
        String hdr = Files.readString(hdrPath, StandardCharsets.UTF_8).trim();
        String[] parts = hdr.split(";", -1);
        if (parts.length != 3) {
            throw new IOException("Cabeçalho .hdr inválido: " + hdr);
        }
        int keyVersion = Integer.parseInt(parts[0]);
        Base64.Decoder b64 = Base64.getDecoder();
        byte[] salt = b64.decode(parts[1]);
        byte[] nonce = b64.decode(parts[2]);
        return new Sealed(chunkId, keyVersion, salt, nonce, ciphertext);
    }

    static byte[] deriveDek(byte[] masterKey, byte[] salt) throws GeneralSecurityException {
        // Extract: PRK = HMAC-SHA256(key = salt, data = masterKey)
        Mac extractMac = Mac.getInstance("HmacSHA256");
        extractMac.init(new SecretKeySpec(salt, "HmacSHA256"));
        byte[] prk = extractMac.doFinal(masterKey);

        // Expand (1 bloco = 32 bytes): T(1) = HMAC-SHA256(key = prk, data = info || 0x01)
        Mac expandMac = Mac.getInstance("HmacSHA256");
        expandMac.init(new SecretKeySpec(prk, "HmacSHA256"));
        expandMac.update(HKDF_INFO);
        expandMac.update((byte) 0x01);
        return expandMac.doFinal();
    }

    private static byte[] buildAad(int keyVersion, byte[] salt, byte[] nonce) {
        ByteBuffer buf = ByteBuffer.allocate(AAD_PREFIX.length + Integer.BYTES + salt.length + nonce.length);
        buf.put(AAD_PREFIX);
        buf.putInt(keyVersion);
        buf.put(salt);
        buf.put(nonce);
        return buf.array();
    }

    private static String sha256Hex(byte[] data) throws GeneralSecurityException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        return HexFormat.of().formatHex(digest.digest(data));
    }
}

package tpf;

public record Sealed(
        String chunkId,
        int keyVersion,
        byte[] salt,
        byte[] nonce,
        byte[] ciphertext
) {
}

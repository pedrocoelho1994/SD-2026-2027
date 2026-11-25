package tpf;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ChunkCryptoTest {

    private byte[] randomBytes(int len) {
        byte[] b = new byte[len];
        new SecureRandom().nextBytes(b);
        return b;
    }

    @Test
    void deveCifrarEDecifrarChunkComPaddingEReconstruirRegistos(@TempDir Path tempDir) throws Exception {
        byte[] mk = randomBytes(ChunkCrypto.KEY_BYTES);

        Chunk chunk = new Chunk();
        Registo r1 = new Registo(1L, Instant.parse("2026-11-24T22:00:00Z"), "martim", "arquivo", "Mensagem secreta 1");
        Registo r2 = new Registo(2L, Instant.parse("2026-11-24T22:00:01Z"), "pedro", "registar", "Texto confidencial 2");
        chunk.adicionar(r1);
        chunk.adicionar(r2);

        byte[] plainPadded = chunk.serializar();
        assertEquals(0, plainPadded.length % Wire.PAD_BYTES, "Tamanho deve ser múltiplo de PAD_BYTES");

        Sealed sealed = ChunkCrypto.seal(plainPadded, mk);

        // Critério 5: nome = SHA-256 do conteúdo cifrado e texto em claro não aparece
        String expectedSha = HexFormat.of().formatHex(
                MessageDigest.getInstance("SHA-256").digest(sealed.ciphertext())
        );
        assertEquals(expectedSha, sealed.chunkId());

        String cipherAsString = new String(sealed.ciphertext(), StandardCharsets.ISO_8859_1);
        assertFalse(cipherAsString.contains("Mensagem secreta 1"));
        assertFalse(cipherAsString.contains("Texto confidencial 2"));

        // Simular gravação em disco (.bin e .hdr) e leitura inversa (Critério 6)
        Path binPath = tempDir.resolve(sealed.chunkId() + ".bin");
        Path hdrPath = tempDir.resolve(sealed.chunkId() + ".hdr");
        Files.write(binPath, sealed.ciphertext());
        Files.writeString(hdrPath, ChunkCrypto.formatHeader(sealed), StandardCharsets.UTF_8);

        Sealed lidoDoDisco = ChunkCrypto.parseFromDisk(sealed.chunkId(), binPath, hdrPath);
        byte[] decrypted = ChunkCrypto.open(lidoDoDisco, mk);
        List<Registo> reconstruidos = Chunk.desserializar(decrypted);
        assertEquals(List.of(r1, r2), reconstruidos);
    }

    @Test
    void deveFalharSeUmByteDoCiphertextForAlterado() throws Exception {
        byte[] mk = randomBytes(ChunkCrypto.KEY_BYTES);
        byte[] dados = new byte[Wire.PAD_BYTES];
        Sealed sealed = ChunkCrypto.seal(dados, mk);

        byte[] adulterado = Arrays.copyOf(sealed.ciphertext(), sealed.ciphertext().length);
        adulterado[0] ^= 0x01;

        Sealed corrompido = new Sealed(sealed.chunkId(), sealed.keyVersion(), sealed.salt(), sealed.nonce(), adulterado);
        assertThrows(Exception.class, () -> ChunkCrypto.open(corrompido, mk));
    }

    @Test
    void deveFalharComSaltNonceOuKeyVersionDiferentes() throws Exception {
        byte[] mk = randomBytes(ChunkCrypto.KEY_BYTES);
        byte[] dados = new byte[Wire.PAD_BYTES];
        Sealed sealed = ChunkCrypto.seal(dados, mk);

        // Salt diferente
        Sealed saltDiferente = new Sealed(sealed.chunkId(), sealed.keyVersion(), randomBytes(32), sealed.nonce(), sealed.ciphertext());
        assertThrows(Exception.class, () -> ChunkCrypto.open(saltDiferente, mk));

        // Nonce diferente
        Sealed nonceDiferente = new Sealed(sealed.chunkId(), sealed.keyVersion(), sealed.salt(), randomBytes(12), sealed.ciphertext());
        assertThrows(Exception.class, () -> ChunkCrypto.open(nonceDiferente, mk));

        // KeyVersion diferente
        Sealed versaoDiferente = new Sealed(sealed.chunkId(), 2, sealed.salt(), sealed.nonce(), sealed.ciphertext());
        assertThrows(Exception.class, () -> ChunkCrypto.open(versaoDiferente, mk));
    }

    @Test
    void deveFalharComMasterKeyDiferente() throws Exception {
        byte[] mk1 = randomBytes(ChunkCrypto.KEY_BYTES);
        byte[] mk2 = randomBytes(ChunkCrypto.KEY_BYTES);
        byte[] dados = new byte[Wire.PAD_BYTES];

        Sealed sealed = ChunkCrypto.seal(dados, mk1);
        assertThrows(Exception.class, () -> ChunkCrypto.open(sealed, mk2));
    }
}

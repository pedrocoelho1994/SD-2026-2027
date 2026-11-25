package tpf;

public final class Wire {

    public static final int CHUNK_MAX_BYTES = Integer.getInteger("tpf.CHUNK_MAX_BYTES", 65536);
    public static final int PAD_BYTES = Integer.getInteger("tpf.PAD_BYTES", 4096);
    public static final long SELAGEM_MS = Long.getLong("tpf.SELAGEM_MS", 5000L);

    static {
        if (CHUNK_MAX_BYTES <= 0 || PAD_BYTES <= 0 || SELAGEM_MS <= 0) {
            throw new IllegalArgumentException(
                    "Parâmetros Wire inválidos: CHUNK_MAX_BYTES, PAD_BYTES e SELAGEM_MS têm de ser > 0."
            );
        }
    }

    private Wire() {
    }
}

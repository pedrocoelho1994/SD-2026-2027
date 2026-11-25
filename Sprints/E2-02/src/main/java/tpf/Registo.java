package tpf;

import java.time.Instant;

public record Registo(
        long lsn,
        Instant instante,
        String autor,
        String canal,
        String texto
) {
}

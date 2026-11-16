
package raft04;

/** Pedido de voto enviado por um candidato numa eleição. */
public final class VoteRequest {

    public final long term;
    public final int candidate;
    public final long lastSeq;
    public final long lastTerm;

    public VoteRequest(long term, int candidate,
                       long lastSeq, long lastTerm) {
        this.term = term;
        this.candidate = candidate;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
    }

    /** Codifica o pedido no formato definido pela COMINDIR05. */
    public String encode() {
        return "PEDIDO_VOTO;" + term + ";" + candidate
                + ";" + lastSeq + ";" + lastTerm;
    }

    /** Interpreta e valida um pedido de voto recebido. */
    public static VoteRequest decode(String message) {
        String[] fields = message.trim().split(";", -1);

        if (fields.length != 5 ||
                !"PEDIDO_VOTO".equals(fields[0])) {
            throw new IllegalArgumentException(
                    "PEDIDO_VOTO mal formado");
        }

        try {
            long term = Long.parseLong(fields[1]);
            int candidate = Integer.parseInt(fields[2]);
            long lastSeq = Long.parseLong(fields[3]);
            long lastTerm = Long.parseLong(fields[4]);

            if (term < 1 || candidate < 1 || candidate > 3 ||
                    lastSeq < 0 || lastTerm < 0 ||
                    lastTerm > term ||
                    (lastSeq == 0 && lastTerm != 0) ||
                    (lastSeq > 0 && lastTerm == 0)) {
                throw new IllegalArgumentException(
                        "PEDIDO_VOTO com valores invalidos");
            }

            return new VoteRequest(
                    term, candidate, lastSeq, lastTerm);

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "PEDIDO_VOTO com numeros invalidos", e);
        }
    }
}

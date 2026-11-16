
package raft04;

/** Resposta a um pedido de voto numa eleição. */
public final class VoteReply {

    public final long term;
    public final int voter;
    public final int candidate;
    public final boolean granted;

    public VoteReply(long term, int voter,
                     int candidate, boolean granted) {
        this.term = term;
        this.voter = voter;
        this.candidate = candidate;
        this.granted = granted;
    }

    /** Codifica a resposta no formato COMINDIR05. */
    public String encode() {
        return "VOTO;" + term + ";" + voter + ";"
                + candidate + ";" + (granted ? "S" : "N");
    }

    /** Interpreta e valida uma resposta recebida. */
    public static VoteReply decode(String message) {
        String[] fields = message.trim().split(";", -1);

        if (fields.length != 5 || !"VOTO".equals(fields[0])) {
            throw new IllegalArgumentException("VOTO mal formado");
        }

        try {
            long term = Long.parseLong(fields[1]);
            int voter = Integer.parseInt(fields[2]);
            int candidate = Integer.parseInt(fields[3]);

            if (term < 1 || voter < 1 || voter > 3 ||
                    candidate < 1 || candidate > 3 ||
                    (!"S".equals(fields[4]) &&
                     !"N".equals(fields[4]))) {
                throw new IllegalArgumentException(
                        "VOTO com valores invalidos");
            }

            return new VoteReply(
                    term,
                    voter,
                    candidate,
                    "S".equals(fields[4])
            );

        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "VOTO com numeros invalidos", e);
        }
    }
}

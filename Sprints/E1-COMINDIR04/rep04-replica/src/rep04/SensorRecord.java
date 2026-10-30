package rep04;

/**
 * Registo normalizado criado pelo líder e replicado para os seus seguidores.
 *
 * O mesmo formato é usado no ficheiro, no tópico PubSub e na recuperação TCP,
 * incluindo os carimbos Lamport e vetorial que explicam a ordem causal.
 */
public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    public final long lamport;
    public final VectorClock vector;

    /** Constrói um registo já completamente carimbado. */
    public SensorRecord(long seq, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    /** Cria um registo com milissegundos para permitir medir a latência do lote. */
    public static SensorRecord now(long seq, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature,
                Long.toString(System.currentTimeMillis()), lamport, vector);
    }

    /** Converte o registo para a linha delimitada persistida e transportada. */
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp
                + ";" + lamport + ";" + vector;
    }

    /** Reconstrói um registo e valida a dimensão causal esperada. */
    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3],
                Long.parseLong(f[4]), VectorClock.parse(f[5], n));
    }

    /** Variante compatível com os ficheiros da configuração predefinida (N=3). */
    public static SensorRecord fromLine(String line) { return fromLine(line, 3); }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
}
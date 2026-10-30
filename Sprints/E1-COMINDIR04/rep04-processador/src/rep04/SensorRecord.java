package rep04;

/** Registo persistido e transportado com seq, dados e carimbos causais. */
public class SensorRecord {
    private final long seq;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;
    public final long lamport;
    public final VectorClock vector;

    /** Constrói um registo que já recebeu todos os seus carimbos. */
    public SensorRecord(long seq, String sensorId, double temperature, String timestamp,
                        long lamport, VectorClock vector) {
        this.seq = seq;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    /** Cria um registo usando o instante local atual. */
    public static SensorRecord now(long seq, String sensorId, double temperature,
                                   long lamport, VectorClock vector) {
        return new SensorRecord(seq, sensorId, temperature,
                Long.toString(System.currentTimeMillis()), lamport, vector);
    }

    /** Formato único usado no ficheiro, PubSub e recuperação TCP. */
    public String toLine() {
        return seq + ";" + sensorId + ";" + temperature + ";" + timestamp
                + ";" + lamport + ";" + vector;
    }

    /** Faz o parsing e valida a dimensão do vetor recebido. */
    public static SensorRecord fromLine(String line, int n) {
        String[] f = line.trim().split(";");
        if (f.length != 6) {
            throw new IllegalArgumentException("Registo mal formado: " + line);
        }
        return new SensorRecord(Long.parseLong(f[0]), f[1], Double.parseDouble(f[2]), f[3],
                Long.parseLong(f[4]), VectorClock.parse(f[5], n));
    }

    public static SensorRecord fromLine(String line) { return fromLine(line, 3); }

    public long getSeq() { return seq; }
    public String getSensorId() { return sensorId; }
    public double getTemperature() { return temperature; }
    public String getTimestamp() { return timestamp; }
}
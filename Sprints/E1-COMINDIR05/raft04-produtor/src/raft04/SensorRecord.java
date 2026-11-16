
package raft04;

/** Registo persistido e transportado com termo, sequência e carimbos causais. */
public class SensorRecord {

    private final long seq;
    private final long term;
    private final String sensorId;
    private final double temperature;
    private final String timestamp;

    public final long lamport;
    public final VectorClock vector;

    /** Constrói um registo com o termo em que foi criado. */
    public SensorRecord(long seq, long term, String sensorId,
                        double temperature, String timestamp,
                        long lamport, VectorClock vector) {

        this.seq = seq;
        this.term = term;
        this.sensorId = sensorId;
        this.temperature = temperature;
        this.timestamp = timestamp;
        this.lamport = lamport;
        this.vector = vector;
    }

    /** Compatibilidade temporária com as classes da ficha anterior. */
    public SensorRecord(long seq, String sensorId, double temperature,
                        String timestamp, long lamport, VectorClock vector) {

        this(seq, 0, sensorId, temperature, timestamp, lamport, vector);
    }

    /** Cria um registo com o termo e o instante atual em milissegundos. */
    public static SensorRecord now(long seq, long term, String sensorId,
                                   double temperature, long lamport,
                                   VectorClock vector) {

        return new SensorRecord(seq, term, sensorId, temperature,
                Long.toString(System.currentTimeMillis()), lamport, vector);
    }

    /** Compatibilidade temporária com a criação de registos anterior. */
    public static SensorRecord now(long seq, String sensorId,
                                   double temperature, long lamport,
                                   VectorClock vector) {

        return now(seq, 0, sensorId, temperature, lamport, vector);
    }

    /** Formato COMINDIR05: seq;termo;sensor;temperatura;instante;L;V */
    public String toLine() {
        return seq + ";" + term + ";" + sensorId + ";" + temperature
                + ";" + timestamp + ";" + lamport + ";" + vector;
    }

    /** Interpreta e valida um registo no novo formato. */
    public static SensorRecord fromLine(String line, int n) {

        String[] fields = line.trim().split(";", -1);

        if (fields.length != 7) {
            throw new IllegalArgumentException(
                    "Registo mal formado: " + line);
        }

        return new SensorRecord(
                Long.parseLong(fields[0]),
                Long.parseLong(fields[1]),
                fields[2],
                Double.parseDouble(fields[3]),
                fields[4],
                Long.parseLong(fields[5]),
                VectorClock.parse(fields[6], n)
        );
    }

    public static SensorRecord fromLine(String line) {
        return fromLine(line, 3);
    }

    public long getSeq() {
        return seq;
    }

    public long getTerm() {
        return term;
    }

    public String getSensorId() {
        return sensorId;
    }

    public double getTemperature() {
        return temperature;
    }

    public String getTimestamp() {
        return timestamp;
    }
}

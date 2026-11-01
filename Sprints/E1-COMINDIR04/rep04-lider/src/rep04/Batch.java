package rep04;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Heartbeat e registos fechados num período do líder. */
public final class Batch {
    public final String address;
    public final int port;
    public final long inc;
    public final long hb;
    public final long lastSeq;
    public final List<SensorRecord> records;

    public Batch(String address, int port, long inc, long hb, long lastSeq,
                 List<SensorRecord> records) {
        this.address = address;
        this.port = port;
        this.inc = inc;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.records = List.copyOf(records);
    }

    public String encode() {
        StringBuilder out = new StringBuilder()
                .append("LOTE;").append(address).append(';').append(port).append(';')
                .append(inc).append(';').append(hb).append(';').append(lastSeq)
                .append(';').append(records.size());
        for (SensorRecord record : records) {
            out.append('\n').append(record.toLine());
        }
        return out.toString();
    }

    public int byteLength() {
        return encode().getBytes(StandardCharsets.UTF_8).length;
    }

    public static Batch decode(String message, int n) {
        String[] lines = message.trim().split("\\R", -1);
        String[] header = lines[0].split(";", -1);
        if (header.length != 7 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("LOTE mal formado");
        }
        int count = Integer.parseInt(header[6]);
        if (count < 0 || lines.length != count + 1) {
            throw new IllegalArgumentException("numero de registos incoerente");
        }
        int port;
        long inc, hb, lastSeq;
        try {
            port = Integer.parseInt(header[2]);
            inc = Long.parseLong(header[3]);
            hb = Long.parseLong(header[4]);
            lastSeq = Long.parseLong(header[5]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("LOTE com numeros invalidos: " + lines[0]);
        }
        if (port <= 0 || inc < 0 || hb < 1 || lastSeq < 0) {
            throw new IllegalArgumentException("LOTE com valores invalidos");
        }
        List<SensorRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            records.add(SensorRecord.fromLine(lines[i + 1], n));
        }
        return new Batch(header[1], port, inc, hb, lastSeq, records);
    }
}
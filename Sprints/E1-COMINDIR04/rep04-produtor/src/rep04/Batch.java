package rep04;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public final class Batch {
    public final String address; public final int port; public final long inc;
    public final long hb; public final long lastSeq; public final List<SensorRecord> records;
    public Batch(String address, int port, long inc, long hb, long lastSeq, List<SensorRecord> records) {
        this.address = address; this.port = port; this.inc = inc; this.hb = hb; this.lastSeq = lastSeq;
        this.records = List.copyOf(records);
    }
    public String encode() {
        StringBuilder out = new StringBuilder("LOTE;").append(address).append(';').append(port)
                .append(';').append(inc).append(';').append(hb).append(';').append(lastSeq)
                .append(';').append(records.size());
        for (SensorRecord r : records) out.append('\n').append(r.toLine());
        return out.toString();
    }
    public int byteLength() { return encode().getBytes(StandardCharsets.UTF_8).length; }
    public static Batch decode(String message, int n) {
        String[] lines = message.trim().split("\\R", -1); String[] h = lines[0].split(";", -1);
        if (h.length != 7 || !"LOTE".equals(h[0])) throw new IllegalArgumentException("LOTE mal formado");
        int count = Integer.parseInt(h[6]);
        if (count < 0 || lines.length != count + 1) throw new IllegalArgumentException("numero de registos incoerente");
        List<SensorRecord> records = new ArrayList<>(count);
        for (int i = 0; i < count; i++) records.add(SensorRecord.fromLine(lines[i + 1], n));
        return new Batch(h[1], Integer.parseInt(h[2]), Long.parseLong(h[3]), Long.parseLong(h[4]),
                Long.parseLong(h[5]), records);
    }
}
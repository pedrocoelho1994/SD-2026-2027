
package raft04;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/** Heartbeat e registos publicados pelo líder de uma partição. */
public final class Batch {

    public final long term;
    public final int leader;
    public final String address;
    public final int port;
    public final long hb;
    public final long lastSeq;
    public final long lastTerm;
    public final List<SensorRecord> records;

    /** Constrói um lote com a identificação da liderança atual. */
    public Batch(long term, int leader, String address, int port,
                 long hb, long lastSeq, long lastTerm,
                 List<SensorRecord> records) {

        this.term = term;
        this.leader = leader;
        this.address = address;
        this.port = port;
        this.hb = hb;
        this.lastSeq = lastSeq;
        this.lastTerm = lastTerm;
        this.records = List.copyOf(records);
    }

    /** Codifica o lote no formato definido para a COMINDIR05. */
    public String encode() {
        StringBuilder out = new StringBuilder()
                .append("LOTE;")
                .append(term).append(';')
                .append(leader).append(';')
                .append(address).append(';')
                .append(port).append(';')
                .append(hb).append(';')
                .append(lastSeq).append(';')
                .append(lastTerm).append(';')
                .append(records.size());

        for (SensorRecord record : records) {
            out.append('\n').append(record.toLine());
        }

        return out.toString();
    }

    /** Calcula o tamanho do lote em bytes UTF-8. */
    public int byteLength() {
        return encode().getBytes(StandardCharsets.UTF_8).length;
    }

    /** Interpreta e valida um lote recebido através de PubSub. */
    public static Batch decode(String message, int n) {

        String[] lines = message.trim().split("\\R", -1);
        String[] header = lines[0].split(";", -1);

        if (header.length != 9 || !"LOTE".equals(header[0])) {
            throw new IllegalArgumentException("LOTE mal formado");
        }

        long term;
        int leader;
        int port;
        long hb;
        long lastSeq;
        long lastTerm;
        int count;

        try {
            term = Long.parseLong(header[1]);
            leader = Integer.parseInt(header[2]);
            port = Integer.parseInt(header[4]);
            hb = Long.parseLong(header[5]);
            lastSeq = Long.parseLong(header[6]);
            lastTerm = Long.parseLong(header[7]);
            count = Integer.parseInt(header[8]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "LOTE com numeros invalidos: " + lines[0], e);
        }

        if (term < 1 || leader < 1 || leader > 3 ||
                header[3].isBlank() || port <= 0 || port > 65535 ||
                hb < 1 || lastSeq < 0 || lastTerm < 0 ||
                count < 0 || lines.length != count + 1) {
            throw new IllegalArgumentException(
                    "LOTE com valores invalidos");
        }

        if ((lastSeq == 0 && lastTerm != 0) ||
                (lastSeq > 0 && lastTerm == 0) ||
                lastTerm > term) {
            throw new IllegalArgumentException(
                    "LOTE com ultimo termo incoerente");
        }

        List<SensorRecord> records = new ArrayList<>(count);

        for (int i = 0; i < count; i++) {
            SensorRecord record =
                    SensorRecord.fromLine(lines[i + 1], n);

            if (record.getTerm() < 1 ||
                    record.getTerm() > term ||
                    record.getSeq() > lastSeq) {
                throw new IllegalArgumentException(
                        "Registo incoerente com o lote");
            }

            records.add(record);
        }

        return new Batch(term, leader, header[3], port,
                hb, lastSeq, lastTerm, records);
    }
}

package rep04;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Persistência append-only de uma partição e leitura para recuperação. */
public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) { path = Paths.get(fileName); }

    /** Acrescenta um registo com fsync (sobrevive a kill -9). */
    public synchronized void append(SensorRecord record) throws IOException {
        byte[] line = (record.toLine() + System.lineSeparator()).getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE,
                StandardOpenOption.WRITE)) {
            long start = channel.size();
            channel.position(start);
            try {
                ByteBuffer buffer = ByteBuffer.wrap(line);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            } catch (IOException failure) {
                channel.truncate(start);
                channel.force(true);
                throw failure;
            }
        }
    }

    /** Regista uma linha em hb-p<k>.txt por cada LOTE publicado. */
    public synchronized void appendHeartbeat(long inc, long hb, long instant, long lastSeq,
                                             int count, int bytes) throws IOException {
        Path heartbeat = Paths.get("hb-p" + partitionFromName(path.getFileName().toString()) + ".txt");
        Files.writeString(heartbeat, inc + ";" + hb + ";" + instant + ";" + lastSeq + ";"
                        + count + ";" + bytes + System.lineSeparator(),
                StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    private static int partitionFromName(String name) {
        int start = name.indexOf("-p");
        int end = name.lastIndexOf(".txt");
        if (start < 0 || end <= start + 2) {
            throw new IllegalArgumentException("Nome de ficheiro de líder inválido: " + name);
        }
        return Integer.parseInt(name.substring(start + 2, end));
    }

    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) return 0;
        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) last = SensorRecord.fromLine(line).getSeq();
        }
        return last;
    }

    public synchronized SensorRecord last(int n) throws IOException {
        if (!Files.exists(path)) return null;
        SensorRecord last = null;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) last = SensorRecord.fromLine(line, n);
        }
        return last;
    }

    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (!Files.exists(path)) return result;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                SensorRecord record = SensorRecord.fromLine(line);
                if (record.getSeq() >= from && record.getSeq() <= to) result.add(record);
            }
        }
        return result;
    }
}
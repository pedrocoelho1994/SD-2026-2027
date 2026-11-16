package raft04;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/** Persistência de registos e substituição atómica para reconciliação. */
public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) { path = Paths.get(fileName); }

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

    public synchronized void appendHeartbeat(long term, long hb, long instant,
            long lastSeq, long lastTerm, int count, int bytes) throws IOException {
        String name = path.getFileName().toString();
        if (!name.startsWith("no-")) {
            throw new IllegalArgumentException("Nome de ficheiro de nó inválido: " + name);
        }
        Path heartbeat = path.resolveSibling("hb-" + name.substring(3));
        String line = term + ";" + hb + ";" + instant + ";" + lastSeq + ";"
                + lastTerm + ";" + count + ";" + bytes + System.lineSeparator();
        Files.writeString(heartbeat, line, StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    public synchronized List<SensorRecord> all() throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        if (!Files.exists(path)) return result;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) result.add(SensorRecord.fromLine(line));
        }
        return result;
    }

    public synchronized long lastSeq() throws IOException {
        List<SensorRecord> data = all();
        return data.isEmpty() ? 0 : data.get(data.size() - 1).getSeq();
    }

    public synchronized SensorRecord last(int n) throws IOException {
        List<SensorRecord> data = all();
        return data.isEmpty() ? null : data.get(data.size() - 1);
    }

    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> result = new ArrayList<>();
        for (SensorRecord record : all()) {
            if (record.getSeq() >= from && record.getSeq() <= to) result.add(record);
        }
        return result;
    }

    public synchronized long lastTerm() throws IOException {
        SensorRecord last = last(0);
        return last == null ? 0 : last.getTerm();
    }

    /** Substitui o ficheiro completo por uma sequência validada, de forma atómica. */
    public synchronized void replaceAll(List<SensorRecord> replacement) throws IOException {
        long expected = 1;
        StringBuilder content = new StringBuilder();
        for (SensorRecord record : replacement) {
            if (record.getSeq() != expected++) {
                throw new IOException("Sequência descontínua na substituição");
            }
            content.append(record.toLine()).append(System.lineSeparator());
        }
        Path parent = path.toAbsolutePath().getParent();
        Path temp = Files.createTempFile(parent, "raft04-", ".tmp");
        try {
            byte[] bytes = content.toString().getBytes(StandardCharsets.UTF_8);
            try (FileChannel channel = FileChannel.open(temp, StandardOpenOption.WRITE)) {
                ByteBuffer buffer = ByteBuffer.wrap(bytes);
                while (buffer.hasRemaining()) channel.write(buffer);
                channel.force(true);
            }
            Files.move(temp, path, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
        } finally {
            Files.deleteIfExists(temp);
        }
    }

    /** Remove os registos com seq >= fromSeq. */
    public synchronized int truncateFrom(long fromSeq) throws IOException {
        if (fromSeq < 1) throw new IllegalArgumentException("Sequência inválida: " + fromSeq);
        List<SensorRecord> data = all();
        List<SensorRecord> kept = new ArrayList<>();
        for (SensorRecord record : data) {
            if (record.getSeq() < fromSeq) kept.add(record);
        }
        int removed = data.size() - kept.size();
        if (removed > 0) replaceAll(kept);
        return removed;
    }
}

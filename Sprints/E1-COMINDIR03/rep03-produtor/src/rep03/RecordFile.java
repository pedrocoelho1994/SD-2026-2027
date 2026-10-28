package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {
    private final Path path;
    private final int n;

    public RecordFile(String fileName, int n) {
        this.path = Paths.get(fileName);
        this.n = n;
    }

    // acrescenta uma linha ao fim do ficheiro (cria-o se nao existir)
    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // ultimo registo guardado (null se o ficheiro nao existir ou estiver vazio)
    public synchronized SensorRecord last() throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        List<String> lines = Files.readAllLines(path, StandardCharsets.UTF_8);
        for (int i = lines.size() - 1; i >= 0; i--) {
            String line = lines.get(i).trim();
            if (!line.isEmpty()) {
                return SensorRecord.fromLine(line, n);
            }
        }
        return null;
    }

    // numero de sequencia do ultimo registo guardado (0 se o ficheiro nao existir)
    public synchronized long lastSeq() throws IOException {
        SensorRecord r = last();
        return r != null ? r.getSeq() : 0;
    }

    // registos com from <= seq <= to, pela ordem do ficheiro (lista vazia se o ficheiro nao existir)
    public synchronized List<SensorRecord> range(long from, long to) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(path)) {
            return out;
        }
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (line.isBlank()) {
                continue;
            }
            SensorRecord r = SensorRecord.fromLine(line, n);
            if (r.getSeq() >= from && r.getSeq() <= to) {
                out.add(r);
            }
        }
        return out;
    }
}

package rep03;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class RecordFile {
    private final Path path;
    private final int n;

    public RecordFile(String fileName) {
        this(fileName, 3); // compatibilidade com o construtor antigo
    }

    public RecordFile(String fileName, int n) {
        if (n < 1) {
            throw new IllegalArgumentException("dimensao do vetor invalida: " + n);
        }
        this.path = Paths.get(fileName);
        this.n = n;
    }

    // acrescenta uma linha ao fim do ficheiro (cria-o se nao existir)
    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // numero de sequencia do ultimo registo guardado (0 se o ficheiro nao existir)
    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }
        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line, n).getSeq();
            }
        }
        return last;
    }

    // ultimo registo guardado (null se o ficheiro nao existir ou estiver vazio).
    // necessario para reinicializar L e V apos reinicio do lider (CA2, ensaio 7d).
    public synchronized SensorRecord last() throws IOException {
        if (!Files.exists(path)) {
            return null;
        }
        SensorRecord last = null;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line, n);
            }
        }
        return last;
    }

    // registos com from <= seq <= to, pela ordem do ficheiro
    // (lista vazia se o ficheiro nao existir)
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
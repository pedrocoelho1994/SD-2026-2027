
package rep01;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;

public class RecordFile {
    private final Path path;

    public RecordFile(String fileName) {
        this.path = Paths.get(fileName);
    }

    // acrescenta uma linha ao fim do ficheiro (cria-o se não existir)
    public synchronized void append(SensorRecord r) throws IOException {
        Files.writeString(path, r.toLine() + System.lineSeparator(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    // número de sequência do último registo guardado (0 se o ficheiro não existir)
    public synchronized long lastSeq() throws IOException {
        if (!Files.exists(path)) {
            return 0;
        }

        long last = 0;
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) {
                last = SensorRecord.fromLine(line).getSeq();
            }
        }
        return last;
    }

    // Procura um registo pelo número de sequência
    public synchronized SensorRecord findBySeq(long seq) throws IOException {

        if (!Files.exists(path)) {
            return null;
        }

        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {

            if (!line.isBlank()) {
                SensorRecord record = SensorRecord.fromLine(line);

                if (record.getSeq() == seq) {
                    return record;
                }
            }
        }

        return null;
    }
}

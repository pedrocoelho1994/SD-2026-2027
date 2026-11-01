package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Verificador offline dos invariantes causais dos ficheiros dos líderes.
 *
 * Além de validar V[k]=seq, compara vetores entre partições, conta relações
 * causais/concorrentes e produz a vista total auxiliar ordenada por (L,k).
 */
public final class CausalCheck {
    private CausalCheck() { }

    /** Liga o registo à partição que o produziu para excluir pares locais. */
    private static final class Entry {
        private final int partition;
        private final SensorRecord record;

        Entry(int partition, SensorRecord record) {
            this.partition = partition;
            this.record = record;
        }

        int partition() { return partition; }
    }

    /** Lê os N ficheiros, gera o relatório causal e a vista Lamport. */
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Uso: CausalCheck <N> <pasta>");
        }
        int n = Integer.parseInt(args[0]);
        Path folder = Paths.get(args[1]);
        List<Entry> entries = new ArrayList<>();
        StringBuilder report = new StringBuilder();
        int invalidLocal = 0;
        // A primeira passagem valida cada relógio local e recolhe todos os eventos.
        for (int k = 0; k < n; k++) {
            Path path = folder.resolve("lider-p" + k + ".txt");
            if (!Files.exists(path)) {
                throw new IOException("Ficheiro de particao em falta: " + path);
            }
            for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
                if (line.isBlank()) continue;
                SensorRecord record = SensorRecord.fromLine(line, n);
                entries.add(new Entry(k, record));
                if (record.vector.get(k) != record.getSeq()) {
                    invalidLocal++;
                    report.append("VIOLACAO V[").append(k).append("] != seq: ")
                            .append(record.toLine()).append(System.lineSeparator());
                }
            }
        }

        long before = 0, after = 0, concurrent = 0, equal = 0;
        long lamportViolations = 0, concurrentDifferentLamport = 0;
        // A segunda passagem classifica apenas pares de partições diferentes.
        for (int i = 0; i < entries.size(); i++) {
            for (int j = i + 1; j < entries.size(); j++) {
                Entry a = entries.get(i);
                Entry b = entries.get(j);
                if (a.partition == b.partition) continue;
                VectorClock.Order order = a.record.vector.compare(b.record.vector);
                switch (order) {
                    case BEFORE:
                        before++;
                        if (a.record.lamport >= b.record.lamport) lamportViolations++;
                        break;
                    case AFTER:
                        after++;
                        if (b.record.lamport >= a.record.lamport) lamportViolations++;
                        break;
                    case CONCURRENT:
                        concurrent++;
                        if (a.record.lamport != b.record.lamport) concurrentDifferentLamport++;
                        break;
                    case EQUAL:
                        equal++;
                        break;
                }
            }
        }
        long causal = before + after;
        report.append("Registos: ").append(entries.size()).append(System.lineSeparator())
                .append("V[k] = seq invalidos: ").append(invalidLocal).append(System.lineSeparator())
                .append("Pares causais: ").append(causal).append(System.lineSeparator())
                .append("  a -> b: ").append(before).append(System.lineSeparator())
                .append("  b -> a: ").append(after).append(System.lineSeparator())
                .append("Pares concorrentes: ").append(concurrent).append(System.lineSeparator())
                .append("Pares iguais: ").append(equal).append(System.lineSeparator())
                .append("Violacoes de Lamport: ").append(lamportViolations).append(System.lineSeparator())
                .append("Concorrentes com L distinto: ").append(concurrentDifferentLamport)
                .append(" (").append(concurrent == 0 ? 0 : (100.0 * concurrentDifferentLamport / concurrent))
                .append("%)").append(System.lineSeparator());

        Files.writeString(folder.resolve("relatorio-causal.txt"), report.toString(),
                StandardCharsets.UTF_8);
        // (Lamport, partição, seq) é uma ordem total determinística; não afirma
        // causalidade para eventos concorrentes, apenas os desempata.
        entries.sort(Comparator.comparingLong((Entry e) -> e.record.lamport)
                .thenComparingInt(Entry::partition)
                .thenComparingLong(e -> e.record.getSeq()));
        List<String> view = new ArrayList<>();
        for (Entry entry : entries) {
            view.add("p" + entry.partition + ";" + entry.record.toLine());
        }
        Files.write(folder.resolve("vista-lamport.txt"), view, StandardCharsets.UTF_8);
        System.out.print(report);
    }
}
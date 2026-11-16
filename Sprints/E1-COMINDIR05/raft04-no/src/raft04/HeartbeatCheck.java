package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

/** Analise offline dos heartbeats, termos e registos da E1-COMINDIR05. */
public final class HeartbeatCheck {
    private static final int NOS_POR_PARTICAO = 3;

    private HeartbeatCheck() { }

    private static final class Heartbeat {
        final int node;
        final long term;
        final long number;
        final long instant;
        final long lastSeq;
        final long lastTerm;
        final int count;
        final int bytes;

        Heartbeat(String line, int node) {
            String[] fields = line.trim().split(";", -1);
            if (fields.length != 7) {
                throw new IllegalArgumentException("Heartbeat mal formado: " + line);
            }
            this.node = node;
            term = Long.parseLong(fields[0]);
            number = Long.parseLong(fields[1]);
            instant = Long.parseLong(fields[2]);
            lastSeq = Long.parseLong(fields[3]);
            lastTerm = Long.parseLong(fields[4]);
            count = Integer.parseInt(fields[5]);
            bytes = Integer.parseInt(fields[6]);
            if (term < 0 || number < 1 || instant < 0 || lastSeq < 0
                    || lastTerm < 0 || count < 0 || bytes < 0) {
                throw new IllegalArgumentException("Heartbeat com valores invalidos: " + line);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Uso: HeartbeatCheck <numero_particoes> <pasta>");
        }
        int partitions = Integer.parseInt(args[0]);
        if (partitions <= 0) {
            throw new IllegalArgumentException("O numero de particoes deve ser positivo.");
        }
        Path root = Paths.get(args[1]).toAbsolutePath().normalize();
        if (!Files.isDirectory(root)) {
            throw new IOException("Pasta inexistente: " + root);
        }

        StringBuilder report = new StringBuilder();
        report.append("E1-COMINDIR05 - Relatorio de analise dos nos e heartbeats\n");
        report.append("Pasta: ").append(root).append("\n");
        report.append("Particoes: ").append(partitions)
                .append(" | Nos por particao: ").append(NOS_POR_PARTICAO).append("\n");
        report.append("Nota: alteracoes de termo nao comprovam, por si so, uma eleicao concluida.\n");
        report.append("Nota: nao ha medicao de latencia de replicacao nem de deteccao de falhas.\n\n");

        for (int partition = 0; partition < partitions; partition++) {
            appendPartitionReport(report, root, partition);
        }

        Path output = root.resolve("relatorio-heartbeat.txt");
        Files.writeString(output, report.toString(), StandardCharsets.UTF_8);
        System.out.print(report);
        System.out.println("Relatorio guardado em: " + output);
    }

    private static void appendPartitionReport(StringBuilder report, Path root, int partition)
            throws IOException {
        List<Heartbeat> heartbeats = new ArrayList<>();
        List<List<String>> replicas = new ArrayList<>();

        for (int node = 1; node <= NOS_POR_PARTICAO; node++) {
            Path hbFile = root.resolve("hb-p" + partition + "-n" + node + ".txt");
            Path recordFile = root.resolve("no-p" + partition + "-n" + node + ".txt");
            if (!Files.isRegularFile(hbFile)) {
                throw new IOException("Ficheiro de heartbeat em falta: " + hbFile);
            }
            if (!Files.isRegularFile(recordFile)) {
                throw new IOException("Ficheiro de registos em falta: " + recordFile);
            }

            for (String line : Files.readAllLines(hbFile, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    heartbeats.add(new Heartbeat(line, node));
                }
            }

            List<String> lines = new ArrayList<>();
            for (String line : Files.readAllLines(recordFile, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) {
                    // Valida o formato de cada registo antes da comparacao entre replicas.
                    SensorRecord.fromLine(line);
                    lines.add(line.trim());
                }
            }
            replicas.add(lines);
        }

        long empty = heartbeats.stream().filter(h -> h.count == 0).count();
        long totalBytes = heartbeats.stream().mapToLong(h -> h.bytes).sum();
        long reportedRecords = heartbeats.stream().mapToLong(h -> h.count).sum();
        int maxCount = heartbeats.stream().mapToInt(h -> h.count).max().orElse(0);

        report.append("Particao p").append(partition).append('\n');
        report.append(String.format(Locale.ROOT,
                "  heartbeats=%d vazios=%d (%.3f) registos_indicados_por_heartbeat=%.3f max_registos=%d%n",
                heartbeats.size(), empty, ratio(empty, heartbeats.size()),
                ratio(reportedRecords, heartbeats.size()), maxCount));
        report.append(String.format(Locale.ROOT,
                "  bytes_totais_heartbeats=%d tamanho_medio_heartbeat_bytes=%.3f%n",
                totalBytes, ratio(totalBytes, heartbeats.size())));

        Map<Integer, Map<Long, List<Heartbeat>>> byNodeAndTerm = new TreeMap<>();
        for (Heartbeat hb : heartbeats) {
            byNodeAndTerm.computeIfAbsent(hb.node, ignored -> new TreeMap<>())
                    .computeIfAbsent(hb.term, ignored -> new ArrayList<>()).add(hb);
        }

        long allIntervals = 0;
        long totalIntervalMs = 0;
        long maxIntervalMs = 0;
        for (Map.Entry<Integer, Map<Long, List<Heartbeat>>> nodeEntry : byNodeAndTerm.entrySet()) {
            for (Map.Entry<Long, List<Heartbeat>> termEntry : nodeEntry.getValue().entrySet()) {
                List<Heartbeat> group = termEntry.getValue();
                group.sort(Comparator.comparingLong((Heartbeat h) -> h.instant)
                        .thenComparingLong(h -> h.number));
                long groupEmpty = group.stream().filter(h -> h.count == 0).count();
                long groupRecords = group.stream().mapToLong(h -> h.count).sum();
                long groupIntervalMs = 0;
                long groupMaxIntervalMs = 0;
                for (int i = 1; i < group.size(); i++) {
                    long interval = group.get(i).instant - group.get(i - 1).instant;
                    groupIntervalMs += interval;
                    groupMaxIntervalMs = Math.max(groupMaxIntervalMs, interval);
                    totalIntervalMs += interval;
                    maxIntervalMs = Math.max(maxIntervalMs, interval);
                    allIntervals++;
                }
                report.append(String.format(Locale.ROOT,
                        "  n%d-termo%d: heartbeats=%d vazios=%.3f registos/heartbeat=%.3f "
                                + "intervalo_medio_ms=%.3f intervalo_max_ms=%d%n",
                        nodeEntry.getKey(), termEntry.getKey(), group.size(),
                        ratio(groupEmpty, group.size()), ratio(groupRecords, group.size()),
                        ratio(groupIntervalMs, group.size() - 1), groupMaxIntervalMs));
            }
        }
        report.append(String.format(Locale.ROOT,
                "  intervalo_global_medio_ms=%.3f intervalo_global_max_ms=%d%n",
                ratio(totalIntervalMs, allIntervals), maxIntervalMs));

        for (int node = 1; node <= NOS_POR_PARTICAO; node++) {
            report.append("  registos_no_n").append(node).append('=')
                    .append(replicas.get(node - 1).size()).append('\n');
        }
        boolean consistent = replicas.get(0).equals(replicas.get(1))
                && replicas.get(0).equals(replicas.get(2));
        report.append("  replicas_identicas=").append(consistent ? "SIM" : "NAO").append('\n');
        if (!consistent) {
            report.append("  AVISO: os ficheiros de registos das replicas diferem.\n");
        }
        report.append('\n');
    }

    private static double ratio(long numerator, long denominator) {
        return denominator <= 0 ? 0.0 : (double) numerator / denominator;
    }
}

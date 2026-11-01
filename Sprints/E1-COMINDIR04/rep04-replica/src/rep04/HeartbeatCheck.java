package rep04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.time.LocalDateTime;
import java.time.ZoneId;

/** Análise offline exigida pelos ensaios da COMINDIR04. */
public final class HeartbeatCheck {
    private HeartbeatCheck() { }

    private static final String REAL = "real";
    private static final String FALSA = "falsa";
    private static final String INICIAL = "inicial";

    private static final class Heartbeat {
        final long inc;
        final long hb;
        final long instant;
        final long lastSeq;
        final int count;
        final int bytes;

        Heartbeat(String line) {
            String[] fields = line.split(";", -1);
            if (fields.length != 6) throw new IllegalArgumentException("heartbeat mal formado: " + line);
            inc = Long.parseLong(fields[0]);
            hb = Long.parseLong(fields[1]);
            instant = Long.parseLong(fields[2]);
            lastSeq = Long.parseLong(fields[3]);
            count = Integer.parseInt(fields[4]);
            bytes = Integer.parseInt(fields[5]);
            if (inc < 0 || hb < 1 || lastSeq < 0 || count < 0 || bytes < 0) {
                throw new IllegalArgumentException("heartbeat com valores inválidos: " + line);
            }
        }
    }

    private static final class MonitorEvent {
        final String source;
        final long instant;
        final String event;
        final int partition;
        final Map<String, String> fields;

        MonitorEvent(String source, String line) {
            this.source = source.replaceFirst("^monitor-", "").replaceFirst("\\.txt$", "");
            String[] fields = line.split(";", -1);
            if (fields.length < 3) throw new IllegalArgumentException("evento de monitor mal formado: " + line);
            instant = Long.parseLong(fields[0]);
            event = fields[1];
            if (!fields[2].startsWith("p")) throw new IllegalArgumentException("partição ausente: " + line);
            partition = Integer.parseInt(fields[2].substring(1));
            this.fields = new HashMap<>();
            for (int i = 3; i < fields.length; i++) {
                int separator = fields[i].indexOf('=');
                if (separator > 0) this.fields.put(fields[i].substring(0, separator), fields[i].substring(separator + 1));
            }
        }
    }

    /** Só os campos que o relatório efetivamente lê. */
    private static final class RecordInfo {
        final long seq;
        final long instant;
        final String line;

        RecordInfo(SensorRecord record) {
            seq = record.getSeq();
            instant = parseInstant(record.getTimestamp());
            line = record.toLine();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 3) {
            throw new IllegalArgumentException("Uso: HeartbeatCheck <N> <pasta> [falhas.txt]");
        }
        int n = Integer.parseInt(args[0]);
        Path root = Paths.get(args[1]).toAbsolutePath().normalize();
        StringBuilder report = new StringBuilder("HeartbeatCheck\n");
        Map<Integer, List<Heartbeat>> heartbeats = readHeartbeats(root, n);
        Map<Integer, List<RecordInfo>> records = readLeaderRecords(root, n);
        for (int k = 0; k < n; k++) appendPartitionReport(report, k, heartbeats.get(k), records.get(k));

        List<MonitorEvent> events = readMonitorEvents(root);
        appendMonitorReport(report, events, heartbeats);
        appendLatencyReport(report, events, records);
        appendFailureReport(report, events, args.length == 3 ? root.resolve(args[2]) : null);

        Files.writeString(root.resolve("relatorio-heartbeat.txt"), report.toString(),
                StandardCharsets.UTF_8);
        System.out.print(report);
    }

    private static Map<Integer, List<Heartbeat>> readHeartbeats(Path root, int n) throws IOException {
        Map<Integer, List<Heartbeat>> result = new HashMap<>();
        for (int k = 0; k < n; k++) {
            Path file = find(root, "hb-p" + k + ".txt");
            List<Heartbeat> values = new ArrayList<>();
            if (file != null) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (!line.isBlank()) values.add(new Heartbeat(line));
                }
            }
            values.sort(Comparator.comparingLong(h -> h.instant));
            result.put(k, values);
        }
        return result;
    }

    private static Map<Integer, List<RecordInfo>> readLeaderRecords(Path root, int n) throws IOException {
        Map<Integer, List<RecordInfo>> result = new HashMap<>();
        for (int k = 0; k < n; k++) {
            Path file = find(root, "lider-p" + k + ".txt");
            List<RecordInfo> values = new ArrayList<>();
            if (file != null) {
                for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                    if (!line.isBlank()) values.add(new RecordInfo(SensorRecord.fromLine(line, n)));
                }
            }
            result.put(k, values);
        }
        return result;
    }

    private static List<MonitorEvent> readMonitorEvents(Path root) throws IOException {
        List<MonitorEvent> result = new ArrayList<>();
        for (Path file : findAll(root, "monitor-")) {
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (!line.isBlank()) result.add(new MonitorEvent(file.getFileName().toString(), line));
            }
        }
        result.sort(Comparator.comparingLong(event -> event.instant));
        return result;
    }

    private static void appendPartitionReport(StringBuilder report, int partition,
                                              List<Heartbeat> values, List<RecordInfo> records) {
        long totalRecords = values.stream().mapToLong(h -> h.count).sum();
        long empty = values.stream().filter(h -> h.count == 0).count();
        long totalInterval = 0;
        long maxInterval = 0;
        int intervals = 0;
        for (int i = 1; i < values.size(); i++) {
            Heartbeat previous = values.get(i - 1);
            Heartbeat current = values.get(i);
            if (previous.inc == current.inc) {
                long interval = current.instant - previous.instant;
                totalInterval += interval;
                maxInterval = Math.max(maxInterval, interval);
                intervals++;
            }
        }
        Map<Long, List<Heartbeat>> byInc = values.stream()
                .collect(Collectors.groupingBy(h -> h.inc));
        report.append(String.format(Locale.ROOT,
                "p%d: lotes=%d vazios=%d (%.3f) registos/lote=%.3f max=%d "
                        + "intervalo_ms=%.3f max=%d%n",
                partition, values.size(), empty,
                values.isEmpty() ? 0 : (double) empty / values.size(),
                values.isEmpty() ? 0 : (double) totalRecords / values.size(),
                values.stream().mapToInt(h -> h.count).max().orElse(0),
                intervals == 0 ? 0 : (double) totalInterval / intervals, maxInterval));
        for (Map.Entry<Long, List<Heartbeat>> entry : byInc.entrySet()) {
            List<Heartbeat> incarnation = entry.getValue();
            long incarnationRecords = incarnation.stream().mapToLong(h -> h.count).sum();
            long incarnationEmpty = incarnation.stream().filter(h -> h.count == 0).count();
            long intervalTotal = 0;
            long intervalMax = 0;
            for (int i = 1; i < incarnation.size(); i++) {
                long interval = incarnation.get(i).instant - incarnation.get(i - 1).instant;
                intervalTotal += interval;
                intervalMax = Math.max(intervalMax, interval);
            }
            report.append(String.format(Locale.ROOT,
                    "  inc=%d lotes=%d vazios=%.3f registos/lote=%.3f max=%d "
                            + "intervalo_ms=%.3f max=%d%n",
                    entry.getKey(), incarnation.size(),
                    incarnation.isEmpty() ? 0 : (double) incarnationEmpty / incarnation.size(),
                    incarnation.isEmpty() ? 0 : (double) incarnationRecords / incarnation.size(),
                    incarnation.stream().mapToInt(h -> h.count).max().orElse(0),
                    incarnation.size() < 2 ? 0 : (double) intervalTotal / (incarnation.size() - 1),
                    intervalMax));
        }

        // Só os lotes com registos: um lote vazio é um heartbeat puro e não transporta dados.
        long batchBytes = values.stream().filter(h -> h.count > 0).mapToLong(h -> h.bytes).sum();
        double batchBytesPerRecord = totalRecords == 0 ? 0 : (double) batchBytes / totalRecords;
        double regBytesPerRecord = records == null || records.isEmpty() ? 0
                : records.stream()
                .mapToInt(r -> ("REG;127.0.0.1;" + (7000 + partition) + ";"
                        + r.line).getBytes(StandardCharsets.UTF_8).length)
                .average().orElse(0);
        report.append(String.format(Locale.ROOT,
                "  bytes/registo: LOTE=%.3f REG-equivalente=%.3f%n",
                batchBytesPerRecord, regBytesPerRecord));
    }

    private static void appendMonitorReport(StringBuilder report, List<MonitorEvent> events,
                                            Map<Integer, List<Heartbeat>> heartbeats) {
        Map<String, Integer> counts = new HashMap<>();
        for (MonitorEvent event : events) counts.merge(event.event, 1, Integer::sum);
        report.append("Eventos:");
        for (String event : List.of("SUSPEITA", "RETOMA", "REINICIO", "HB_PERDIDO", "RECUPERACAO")) {
            report.append(' ').append(event).append('=').append(counts.getOrDefault(event, 0));
        }
        report.append('\n');

        Map<String, Integer> kinds = new HashMap<>();
        for (MonitorEvent event : events) {
            if (!"SUSPEITA".equals(event.event)) continue;
            String kind = classify(event, events);
            kinds.merge(kind, 1, Integer::sum);
            long lastPublished = heartbeats.getOrDefault(event.partition, List.of()).stream()
                    .filter(h -> h.instant <= event.instant)
                    .mapToLong(h -> h.instant).max().orElse(-1);
            report.append("  ").append(kind)
                    .append(" p").append(event.partition)
                    .append(" participante=").append(event.source)
                    .append(" silencio_detetor_ms=").append(event.fields.getOrDefault("silencio_ms", "?"))
                    .append(" silencio_desde_publicado_ms=")
                    .append(lastPublished < 0 ? "?" : Long.toString(event.instant - lastPublished))
                    .append('\n');
        }
        report.append("Suspeitas: reais=").append(kinds.getOrDefault(REAL, 0))
                .append(" falsas=").append(kinds.getOrDefault(FALSA, 0))
                .append(" antes_do_primeiro_heartbeat=").append(kinds.getOrDefault(INICIAL, 0))
                .append('\n');
    }

    private static void appendLatencyReport(StringBuilder report, List<MonitorEvent> events,
                                            Map<Integer, List<RecordInfo>> records) {
        for (Map.Entry<Integer, List<RecordInfo>> entry : records.entrySet()) {
            List<Long> latencies = new ArrayList<>();
            for (MonitorEvent event : events) {
                if (!"RECEBIDO".equals(event.event) || event.partition != entry.getKey()) continue;
                long first = parseLong(event.fields.get("de"), -1);
                long last = parseLong(event.fields.get("ate"), -1);
                if (first < 0 || last < first) continue;
                for (RecordInfo record : entry.getValue()) {
                    if (record.seq >= first && record.seq <= last) latencies.add(event.instant - record.instant);
                }
            }
            report.append(String.format(Locale.ROOT, "p%d: latência_lote_ms=%.3f max=%d%n",
                    entry.getKey(), latencies.stream().mapToLong(Long::longValue).average().orElse(0),
                    latencies.stream().mapToLong(Long::longValue).max().orElse(0)));
        }
    }

    private static void appendFailureReport(StringBuilder report, List<MonitorEvent> events,
                                            Path failures) throws IOException {
        if (failures == null || !Files.exists(failures)) return;
        Map<Integer, List<Long>> byPartition = new HashMap<>();
        for (String line : Files.readAllLines(failures, StandardCharsets.UTF_8)) {
            String[] fields = line.trim().split(";", -1);
            if (fields.length != 2 || !fields[0].startsWith("p")) continue;
            byPartition.computeIfAbsent(Integer.parseInt(fields[0].substring(1)), k -> new ArrayList<>())
                    .add(Long.parseLong(fields[1]));
        }
        List<Long> detections = new ArrayList<>();
        for (Map.Entry<Integer, List<Long>> entry : byPartition.entrySet()) {
            int partition = entry.getKey();
            List<Long> instants = entry.getValue();
            instants.sort(Comparator.naturalOrder());
            for (int i = 0; i < instants.size(); i++) {
                long failure = instants.get(i);
                // Cada falha só conta a primeira suspeita de cada participante antes da falha seguinte.
                long next = i + 1 < instants.size() ? instants.get(i + 1) : Long.MAX_VALUE;
                Set<String> seen = new HashSet<>();
                for (MonitorEvent event : events) {
                    if ("SUSPEITA".equals(event.event) && event.partition == partition
                            && event.instant >= failure && event.instant < next
                            && REAL.equals(classify(event, events)) && seen.add(event.source)) {
                        long detection = event.instant - failure;
                        detections.add(detection);
                        report.append("falha p").append(partition).append(" instante=").append(failure)
                                .append(" participante=").append(event.source)
                                .append(" deteção_ms=").append(detection).append('\n');
                    }
                }
            }
        }
        if (!detections.isEmpty()) {
            report.append(String.format(Locale.ROOT, "Tempo de deteção_ms: min=%d média=%.1f max=%d%n",
                    detections.stream().mapToLong(Long::longValue).min().orElse(0),
                    detections.stream().mapToLong(Long::longValue).average().orElse(0),
                    detections.stream().mapToLong(Long::longValue).max().orElse(0)));
        }
    }

    /**
     * Falsa: a primeira RETOMA seguinte tem o mesmo inc. Real: não há RETOMA ou esta traz outro inc.
     * Inicial: o participante ainda não tinha recebido nenhum heartbeat, logo não há líder a acusar.
     */
    private static String classify(MonitorEvent suspicion, List<MonitorEvent> events) {
        long incarnation = -1;
        for (MonitorEvent event : events) {
            if (!event.source.equals(suspicion.source) || event.partition != suspicion.partition) continue;
            if (event.instant <= suspicion.instant && "RECEBIDO".equals(event.event)) {
                incarnation = parseLong(event.fields.get("inc"), -1);
            }
            if (event.instant > suspicion.instant && "RETOMA".equals(event.event)) {
                if (incarnation == -1) return INICIAL;
                return parseLong(event.fields.get("inc"), -2) == incarnation ? FALSA : REAL;
            }
        }
        return incarnation == -1 ? INICIAL : REAL;
    }

    private static long parseLong(String value, long fallback) {
        if (value == null) return fallback;
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return fallback;
        }
    }

    private static long parseInstant(String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ignored) {
            return LocalDateTime.parse(value).atZone(ZoneId.systemDefault())
                    .toInstant().toEpochMilli();
        }
    }

    private static Path find(Path root, String fileName) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(path -> path.getFileName().toString().equals(fileName))
                    .findFirst().orElse(null);
        }
    }

    private static List<Path> findAll(Path root, String prefix) throws IOException {
        try (Stream<Path> paths = Files.walk(root)) {
            return paths.filter(path -> path.getFileName().toString().startsWith(prefix)
                    && path.getFileName().toString().endsWith(".txt"))
                    .collect(Collectors.toList());
        }
    }
}
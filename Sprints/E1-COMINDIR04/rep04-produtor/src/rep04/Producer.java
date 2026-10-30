package rep04;

import java.util.Random;
import java.util.Properties;
import java.util.Scanner;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.nio.file.Paths;

/** Interface de consola que publica leituras IN no tópico da sua partição. */
public class Producer {
    static final String API = "http://127.0.0.1:5001/api/v0"; // API por omissao
    private static final long RAJADA_DELAY_MS = 6; // for debug purposes, 0 otherwise
    private static HeartbeatMonitor[] monitors;

    /** Lê comandos, gera rajadas opcionais e termina com {@code sair}. */
    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
        // numero de particoes e endereco da API: Producer <N> [api];
        // sem argumentos, os valores sao lidos de runtime.properties
        int n = args.length > 0 ? Integer.parseInt(args[0])
                : Integer.parseInt(RuntimeConfig.value(config, "partitions", "3"));
        String api = args.length > 1 ? args[1] : RuntimeConfig.value(config, "producer.api", API);
        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub pubsub = new IpfsPubSub(api);
        monitors = new HeartbeatMonitor[n];
        for (int k = 0; k < n; k++) {
            final int partition = k;
            monitors[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                    event -> monitorLog(event));
            final HeartbeatMonitor monitor = monitors[k];
            Thread subscription = new Thread(() -> {
                try {
                    new IpfsPubSub(api).subscribe(Wire.topicData(partition), msg -> {
                        Batch batch = Batch.decode(msg, n);
                        monitor.onHeartbeat(batch.inc, batch.hb);
                        long firstSeq = batch.records.isEmpty() ? -1 : batch.records.get(0).getSeq();
                        long lastBatchSeq = batch.records.isEmpty() ? -1
                                : batch.records.get(batch.records.size() - 1).getSeq();
                        monitorLog("RECEBIDO;p" + partition + ";inc=" + batch.inc
                                + ";hb=" + batch.hb + ";ultimoSeq=" + batch.lastSeq
                                + ";n=" + batch.records.size() + ";de=" + firstSeq
                                + ";ate=" + lastBatchSeq);
                    });
                } catch (Exception e) {
                    System.out.println("Subscricao do produtor p" + partition
                            + " terminada: " + e.getMessage());
                }
            }, "produtor-p" + partition);
            subscription.setDaemon(true);
            subscription.start();
        }
        Thread checker = new Thread(() -> {
            while (true) {
                for (HeartbeatMonitor monitor : monitors) monitor.check();
                try { Thread.sleep(Wire.VERIFICACAO_MS); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); return; }
            }
        }, "produtor-heartbeats");
        checker.setDaemon(true);
        checker.start();
        Random rand = new Random(); // sensores e temperaturas aleatorias
        int[] contadores = new int[n]; // leituras publicadas por particao

        System.out.println("Produtor pronto (API: " + api + ", " + n + " particoes).");
        System.out.println("Formato: <sensor> <temperatura> | 'rajada <n> <s>' | 'rajada-quente <n> <s>' | 'sair'");

        try (Scanner sc = new Scanner(System.in)) {
            // ciclo de leitura de comandos: bloqueia a espera de uma linha
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) {
                    break;
                }
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) {
                    break;
                }
                if (line.isEmpty()) {
                    continue; // ignora linhas vazias
                }

                // rajada <n> <s>: publica n leituras, o mais depressa possivel, com sensores aleatorios entre S01 e S<s>
                // rajada-quente <n> <s>: igual, mas 80 % percorre um conjunto quente de sensores
                boolean quente = line.toLowerCase().startsWith("rajada-quente ");
                if (quente || line.toLowerCase().startsWith("rajada ")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length != 3) {
                        System.out.println("Uso: rajada <n> <s> | rajada-quente <n> <s>");
                        continue;
                    }
                    try {
                        int nLeituras = Integer.parseInt(parts[1]); // numero de leituras
                        int s = Integer.parseInt(parts[2]); // sensores de S01 a S<s>
                        if (nLeituras < 1 || s < 1) {
                            System.out.println("Valores invalidos.");
                            continue;
                        }

                        long tInicio = System.currentTimeMillis(); // instante da 1a publicacao
                        long tFim = tInicio;
                        for (int i = 0; i < nLeituras; i++) {
                            String sensor = quente
                                    ? sensorQuente(rand, s, i)
                                    : String.format("S%02d", rand.nextInt(s) + 1); // sensor aleaterio
                            double temp = Math.round((15.0 + 15.0 * rand.nextDouble()) * 10.0) / 10.0; // temperatura aleatoria entre 15 e 30
                            publicar(pubsub, partitioner, contadores, sensor, temp);
                            tFim = System.currentTimeMillis(); // instante da ultima publicacao
                            if (i + 1 < nLeituras) {
                                Thread.sleep(RAJADA_DELAY_MS);
                            }
                        }
                        System.out.println("Rajada concluida: " + nLeituras + " leituras.");
                        for (int k = 0; k < n; k++) {
                            System.out.println("p" + k + ": " + contadores[k] + " leituras");
                        }
                        System.out.println("Primeira: " + tInicio + " ms | ultima: " + tFim + " ms | Duracao: "
                                + (tFim - tInicio) + " ms");
                    } catch (NumberFormatException e) {
                        System.out.println("rajada: n e s tem de ser inteiros.");
                    }
                    continue;
                }

                // publica uma leitura
                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato invalido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]); // converte o texto em numero
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura invalida: " + p[1]);
                    continue;
                }
                publicar(pubsub, partitioner, contadores, p[0], temp); // publica IN no topico de entrada da particao do sensor
            }
        } catch (Exception e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }

    /**
     * Publica uma leitura com L=0 e vetor zero: o produtor não é um líder nem
     * cria eventos persistidos, logo não acrescenta uma componente causal.
     */
    static void publicar(IpfsPubSub ps, Partitioner pt, int[] contadores, String sensor, double temp) throws Exception {
        int k = pt.of(sensor);
        if (monitors != null && monitors[k].state() == HeartbeatMonitor.State.SUSPEITO) {
            System.out.println("AVISO: líder p" + k + " suspeito; leitura "
                    + sensor + " pode perder-se");
        }
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp, 0, new VectorClock(pt.count())));
        contadores[k]++;
        System.out.println("Publicado: " + Wire.in(sensor, temp, 0, new VectorClock(pt.count())) + " -> p" + k);
    }

    private static synchronized void monitorLog(String event) {
        try {
            Files.writeString(Paths.get("monitor-produtor.txt"),
                    System.currentTimeMillis() + ";" + event + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (Exception e) {
            System.out.println("Falha no monitor-produtor: " + e.getMessage());
        }
        System.out.println("MONITOR: " + event);
    }

    /** Concentra 80% das rajadas em S01..S03 para exercitar alarmes cruzados. */
    static String sensorQuente(Random rand, int s, int indice) {
        int tamanhoConjuntoQuente = Math.min(s, 3);
        if (rand.nextDouble() < 0.8) {
            return String.format("S%02d", (indice % tamanhoConjuntoQuente) + 1);
        }
        if (s > tamanhoConjuntoQuente) {
            return String.format("S%02d",
                    rand.nextInt(s - tamanhoConjuntoQuente) + tamanhoConjuntoQuente + 1);
        }
        return String.format("S%02d", (indice % tamanhoConjuntoQuente) + 1);
    }
}
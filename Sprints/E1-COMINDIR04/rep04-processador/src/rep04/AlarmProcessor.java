package rep04;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Observa todas as partições e transforma leituras quentes em novas IN.
 *
 * O processador é o elo causal entre partições: funde L/V dos LOTEs observados,
 * tira um snapshot e publica ALARM- no tópico da partição do sensor sintético.
 */
public final class AlarmProcessor {
    private AlarmProcessor() {
    }

    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
        int n = args.length > 0 ? Integer.parseInt(args[0])
                : Integer.parseInt(RuntimeConfig.value(config, "partitions", "3"));
        String api = args.length > 1 ? args[1]
                : RuntimeConfig.value(config, "leader.api", "http://127.0.0.1:5002/api/v0");
        Partitioner partitioner = new Partitioner(n);
        Processor processor = new Processor(n, api, partitioner);
        processor.start();
    }

    private static final class Processor {
        private final int n;
        private final String api;
        private final Partitioner partitioner;
        private final Object lock = new Object();
        private long lp;
        private final VectorClock vp;
        private final Set<String> seen;
        private final HeartbeatMonitor[] monitors;
        private final ExecutorService alarmPublisher = Executors.newSingleThreadExecutor(r -> {
            Thread thread = new Thread(r, "processador-alarmes");
            thread.setDaemon(true);
            return thread;
        });

        Processor(int n, String api, Partitioner partitioner) {
            this.n = n;
            this.api = api;
            this.partitioner = partitioner;
            this.lp = 0;
            this.vp = new VectorClock(n);
            this.seen = new HashSet<>();
            this.monitors = new HeartbeatMonitor[n];
            for (int k = 0; k < n; k++) {
                monitors[k] = new HeartbeatMonitor(k, Wire.PERIODO_HB_MS, Wire.FATOR_TIMEOUT,
                        event -> monitorLog(event));
            }
        }

        void start() throws InterruptedException {
            Thread[] threads = new Thread[n];
            for (int k = 0; k < n; k++) {
                final int partition = k;
                threads[k] = new Thread(() -> subscribe(partition), "processador-p" + k);
                threads[k].start();
            }
            ScheduledExecutorService checks = Executors.newSingleThreadScheduledExecutor();
            checks.scheduleAtFixedRate(() -> {
                for (HeartbeatMonitor monitor : monitors) monitor.check();
            }, Wire.VERIFICACAO_MS, Wire.VERIFICACAO_MS, TimeUnit.MILLISECONDS);
            for (Thread thread : threads) {
                thread.join();
            }
        }

        private void subscribe(int k) {
            try {
                new IpfsPubSub(api).subscribe(Wire.topicData(k), msg -> process(k, msg));
            } catch (Exception e) {
                System.out.println("Subscricao do processador p" + k + " terminada: " + e.getMessage());
            }
        }

        /** Valida um LOTE, elimina duplicados e agenda eventual alarme. */
        private void process(int k, String msg) {
            try {
                Batch batch = Batch.decode(msg, n);
                monitors[k].onHeartbeat(batch.inc, batch.hb);
                long firstSeq = batch.records.isEmpty() ? -1 : batch.records.get(0).getSeq();
                long lastBatchSeq = batch.records.isEmpty() ? -1
                        : batch.records.get(batch.records.size() - 1).getSeq();
                monitorLog("RECEBIDO;p" + k + ";inc=" + batch.inc + ";hb=" + batch.hb
                        + ";ultimoSeq=" + batch.lastSeq + ";n=" + batch.records.size()
                        + ";de=" + firstSeq + ";ate=" + lastBatchSeq);
                for (SensorRecord record : batch.records) {
                    if (record.getSensorId().startsWith("ALARM-")) continue;
                    String alarm = null;
                    int target = -1;
                    synchronized (lock) {
                        if (!seen.add(k + "-" + record.getSeq())) continue;
                        lp = Math.max(lp, record.lamport);
                        vp.merge(record.vector);
                        if (record.getTemperature() >= Wire.LIMIAR_ALARME) {
                            String alarmSensor = "ALARM-" + record.getSensorId();
                            target = partitioner.of(alarmSensor);
                            alarm = Wire.in(alarmSensor, record.getTemperature(), lp, vp.copy());
                            final String finalAlarm = alarm;
                            final int finalTarget = target;
                            final HeartbeatMonitor targetMonitor = monitors[finalTarget];
                            alarmPublisher.execute(() -> {
                                try {
                                    if (targetMonitor.state() == HeartbeatMonitor.State.SUSPEITO) {
                                        System.out.println("AVISO: líder p" + finalTarget
                                                + " suspeito; alarme " + record.getSensorId() + " pode perder-se");
                                    }
                                    new IpfsPubSub(api).publish(Wire.topicIn(finalTarget), finalAlarm);
                                    System.out.println("ALARME: " + record.getSensorId() + " p" + k
                                            + "#" + record.getSeq() + " -> p" + finalTarget);
                                } catch (Exception e) {
                                    System.out.println("Erro ao publicar o alarme: " + e.getMessage());
                                }
                            });
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("REJEITADO pelo processador: " + msg.trim() + " (" + e.getMessage() + ")");
            }
        }

        private static synchronized void monitorLog(String event) {
            try {
                Files.writeString(Paths.get("monitor-processador.txt"),
                        System.currentTimeMillis() + ";" + event + System.lineSeparator(),
                        StandardCharsets.UTF_8, java.nio.file.StandardOpenOption.CREATE,
                        java.nio.file.StandardOpenOption.APPEND);
            } catch (Exception e) {
                System.out.println("Falha no monitor-processador: " + e.getMessage());
            }
            System.out.println("MONITOR: " + event);
        }
    }
}
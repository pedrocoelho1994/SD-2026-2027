package rep03;

import java.util.Properties;

public class AlarmProcessor {
    private static final Object LOCK = new Object();
    private static long lp = 0L;
    private static VectorClock vp;
    private static long[] lastSeq;

    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
        int n = args.length > 0 ? Integer.parseInt(args[0])
                : Integer.parseInt(RuntimeConfig.value(config, "partitions", "3"));
        String api = args.length > 1 ? args[1]
                : RuntimeConfig.value(config, "leader.api", "http://127.0.0.1:5001/api/v0");

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);

        vp = new VectorClock(n);
        lastSeq = new long[n];

        System.out.println("Processador de Alarmes pronto (N=" + n + ", API: " + api + ").");
        System.out.println("Limiar de alarme: " + Wire.LIMIAR_ALARME + " ºC");

        Thread[] threads = new Thread[n];
        for (int i = 0; i < n; i++) {
            final int k = i;
            threads[k] = new Thread(() -> {
                try {
                    System.out.println("Subscrevendo particao p" + k + " em " + Wire.topicData(k));
                    ps.subscribe(Wire.topicData(k), msg -> {
                        try {
                            String[] parts = Wire.regParts(msg);
                            SensorRecord r = SensorRecord.fromLine(parts[2], n);

                            // Ignora registos de alarmes para evitar alarmes sobre alarmes
                            if (r.getSensorId().startsWith("ALARM-")) {
                                return;
                            }

                            long lEnvio;
                            VectorClock vEnvio;
                            boolean gerarAlarme = false;
                            String sensorAlarme = null;
                            int pj = -1;

                            synchronized (LOCK) {
                                // Ignora registos duplicados (mesma particao e mesmo seq)
                                if (r.getSeq() <= lastSeq[k]) {
                                    return;
                                }
                                lastSeq[k] = r.getSeq();

                                // Acumula o conhecimento causal com o maximo de cada registo recebido
                                lp = Math.max(lp, r.getLamport());
                                vp.merge(r.getVector());

                                if (r.getTemperature() >= Wire.LIMIAR_ALARME) {
                                    gerarAlarme = true;
                                    sensorAlarme = "ALARM-" + r.getSensorId();
                                    pj = partitioner.of(sensorAlarme);
                                    lEnvio = lp;
                                    vEnvio = vp.copy();
                                    System.out.println("ALARME: " + r.getSensorId() + " p" + k + "#" + r.getSeq() + " -> p" + pj);
                                } else {
                                    lEnvio = 0L;
                                    vEnvio = null;
                                }
                            }

                            if (gerarAlarme) {
                                ps.publish(Wire.topicIn(pj), Wire.in(sensorAlarme, r.getTemperature(), lEnvio, vEnvio));
                            }
                        } catch (Exception e) {
                            System.err.println("[p" + k + "] Erro ao processar REG: " + e.getMessage());
                        }
                    });
                } catch (Exception e) {
                    System.err.println("Subscricao p" + k + " terminada: " + e.getMessage());
                }
            }, "alarm-proc-p" + k);
            threads[k].start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }
}

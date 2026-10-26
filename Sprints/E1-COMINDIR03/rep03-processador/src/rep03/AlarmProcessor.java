package rep03;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

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

        Processor(int n, String api, Partitioner partitioner) {
            this.n = n;
            this.api = api;
            this.partitioner = partitioner;
            this.lp = 0;
            this.vp = new VectorClock(n);
            this.seen = new HashSet<>();
        }

        void start() throws InterruptedException {
            Thread[] threads = new Thread[n];
            for (int k = 0; k < n; k++) {
                final int partition = k;
                threads[k] = new Thread(() -> subscribe(partition), "processador-p" + k);
                threads[k].start();
            }
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

        private void process(int k, String msg) {
            try {
                String[] parts = Wire.regParts(msg);
                SensorRecord record = SensorRecord.fromLine(parts[2], n);
                // Mitigação de flooding em rede (evitar alarmes sobre alarmes)
                if (record.getSensorId().startsWith("ALARM-")) {
                    return;
                }
                String alarm = null;
                int target = -1;
                synchronized (lock) {
                    if (!seen.add(k + "-" + record.getSeq())) {
                        return;
                    }
                    lp = Math.max(lp, record.lamport);
                    vp.merge(record.vector);
                    if (record.getTemperature() >= Wire.LIMIAR_ALARME) {
                        String alarmSensor = "ALARM-" + record.getSensorId();
                        target = partitioner.of(alarmSensor);
                        alarm = Wire.in(alarmSensor, record.getTemperature(), lp, vp.copy());
                    }
                }
                if (alarm != null) {
                    final String finalAlarm = alarm;
                    final int finalTarget = target;
                    // Nova thread para não bloquear no pedido HTTP sincrono
                    new Thread(() -> {
                        try {
                            new IpfsPubSub(api).publish(Wire.topicIn(finalTarget), finalAlarm);
                            System.out.println("ALARME: " + record.getSensorId() + " p" + k
                                    + "#" + record.getSeq() + " -> p" + finalTarget);
                        } catch (Exception e) {
                            System.out.println("Erro ao publicar o alarme: " + e.getMessage());
                        }
                    }).start();
                }
            } catch (Exception e) {
                System.out.println("REJEITADO pelo processador: " + msg.trim() + " (" + e.getMessage() + ")");
            }
        }

    }
}
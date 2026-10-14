
package rep01;

import java.io.IOException;
import java.util.TreeMap;

public class PubSubReplica {

    private static final String TOPIC = "rep01-temperaturas";
    private static final String DEFAULT_API =
            "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {

        String id = args.length > 0 ? args[0] : "1";
        String port = args.length > 1 ? args[1] : "5001";

        String api = "http://127.0.0.1:" + port + "/api/v0";

        IpfsPubSub pubsub = new IpfsPubSub(api);
        RecordFile file = new RecordFile("replica-" + id + ".txt");

        TreeMap<Long, SensorRecord> pending = new TreeMap<>();

        try {
            System.out.println("Réplica PubSub " + id + " pronta.");
            System.out.println("Ligada à API: " + api);
            System.out.println("À espera de mensagens...");

            pubsub.subscribe(TOPIC, message -> {
                try {
                    SensorRecord record = SensorRecord.fromLine(message);

                    long lastSeq = file.lastSeq();
                    long expectedSeq = lastSeq + 1;
                    long receivedSeq = record.getSeq();

                    if (receivedSeq <= lastSeq
                            || pending.containsKey(receivedSeq)) {

                        System.out.println("DUPLICADO: " + receivedSeq);

                    } else if (receivedSeq > expectedSeq) {

                        pending.put(receivedSeq, record);

                        System.out.println(
                                "EM ESPERA: " + receivedSeq
                                + " (falta o registo " + expectedSeq + ")"
                        );

                    } else {

                        file.append(record);
                        System.out.println("APLICADO: " + record.toLine());

                        long nextSeq = receivedSeq + 1;

                        while (pending.containsKey(nextSeq)) {

                            SensorRecord next = pending.remove(nextSeq);
                            file.append(next);

                            System.out.println(
                                    "APLICADO DA MEMÓRIA: " + next.toLine()
                            );

                            nextSeq++;
                        }
                    }

                } catch (IllegalArgumentException e) {

                    System.out.println("REJEITADO: " + message);

                } catch (IOException e) {

                    System.err.println(
                            "Erro ao escrever no ficheiro: " + e.getMessage()
                    );
                }
            });

        } catch (IOException | InterruptedException e) {

            System.err.println(
                    "Erro na réplica PubSub: " + e.getMessage()
            );

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

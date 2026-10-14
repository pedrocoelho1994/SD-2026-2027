
package rep01;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class IpfsReplica {

    private static final String TOPIC = "rep01-temperaturas";
    private static final String RECOVERY_TOPIC = "rep01-recuperacao";

    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Uso: java rep01.IpfsReplica <id>");
            return;
        }

        String replicaId = args[0];
        RecordFile file = new RecordFile("replica-ipfs-" + replicaId + ".txt");
        TreeMap<Long, SensorRecord> pending = new TreeMap<>();
        

        try {
            Process process = new ProcessBuilder(
                "ipfs", "pubsub", "sub", TOPIC
            ).redirectErrorStream(true).start();

            System.out.println("Réplica IPFS " + replicaId + " pronta.");
            System.out.println("À espera de mensagens no tópico " + TOPIC);

            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(
                        process.getInputStream(),
                        StandardCharsets.UTF_8
                    ))) {

                String message;

                while ((message = reader.readLine()) != null) {

                    try {
                        SensorRecord record = SensorRecord.fromLine(message);

                        long lastSeq = file.lastSeq();

                       long expectedSeq = lastSeq + 1;

                    if (record.getSeq() < expectedSeq) {
                        System.out.println("Registo duplicado ou antigo: " + message);

                    } else if (record.getSeq() > expectedSeq) {
                        pending.putIfAbsent(record.getSeq(), record);

                        System.out.println(
                            "Falha na sequência! Esperado: " + expectedSeq
                            + " | Recebido: " + record.getSeq()
                            + " | Registo colocado em espera."
                        );

                        // Pedir automaticamente ao líder o registo em falta
                        requestRecovery(expectedSeq);

                    } else {
                        file.append(record);
                        System.out.println("Registo guardado: " + message);

                        long nextSeq = record.getSeq() + 1;

                        while (pending.containsKey(nextSeq)) {
                            SensorRecord nextRecord = pending.remove(nextSeq);
                            file.append(nextRecord);

                            System.out.println(
                                "Registo recuperado da memória: " + nextRecord.toLine()
                            );

                            nextSeq++;
                        }
                        // Se ainda existem registos em espera, pedir a próxima sequência em falta
                        if (!pending.isEmpty() && pending.firstKey() > nextSeq) {
                            System.out.println(
                                "Ainda existem registos em espera. A pedir sequência: " + nextSeq
                            );

                            requestRecovery(nextSeq);
                        }
                    }

                    } catch (Exception e) {
                        System.out.println("Mensagem inválida: " + message);
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("Erro na réplica IPFS: " + e.getMessage());
        }
    }

        private static void requestRecovery(long seq) throws Exception {

        Process process = new ProcessBuilder(
            "ipfs", "pubsub", "pub",
            RECOVERY_TOPIC, "-"
        ).start();

        try (java.io.OutputStream output = process.getOutputStream()) {
            output.write(
                (seq + "\n").getBytes(StandardCharsets.UTF_8)
            );
            output.flush();
        }

        int exitCode = process.waitFor();

        if (exitCode != 0) {
            throw new IllegalStateException(
                "Falha ao pedir recuperação da sequência " + seq
            );
        }

        System.out.println(
            "Pedido de recuperação enviado: " + seq
        );
    }
}

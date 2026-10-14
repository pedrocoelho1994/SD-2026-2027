
package rep01;

import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;


public class IpfsLeader {

    private static final String TOPIC = "rep01-temperaturas";
    private static final String RECOVERY_TOPIC = "rep01-recuperacao";
    private static final String API = "http://127.0.0.1:5001";


    public static void main(String[] args) {
        RecordFile file = new RecordFile("lider-ipfs.txt");

        try {
            // Calcular o próximo número de sequência
            long nextSeq = file.lastSeq() + 1;

            // Criar um novo registo de temperatura
            String message = nextSeq
                    + ";S01;22.5;"
                    + java.time.LocalDateTime.now();

            SensorRecord record = SensorRecord.fromLine(message);

            file.append(record);
            publish(message);

            System.out.println("Registo guardado e publicado: " + message);
            // Manter o líder à escuta dos pedidos de recuperação
            listenForRecoveryRequests();

        } catch (Exception e) {
            System.err.println("Erro no líder IPFS: " + e.getMessage());
        }
    }

    private static void publish(String message) throws Exception {
        Path tempFile = Files.createTempFile("rep01-ipfs-", ".txt");

        try {
            Files.writeString(tempFile, message + "\n", StandardCharsets.UTF_8);

            Process process = new ProcessBuilder(
                "ipfs", "pubsub", "pub", TOPIC, tempFile.toString()
            ).inheritIO().start();

            int exitCode = process.waitFor();

            if (exitCode != 0) {
                throw new IllegalStateException(
                    "Falha na publicação IPFS. Código: " + exitCode
                );
            }
        } finally {
            Files.deleteIfExists(tempFile);
        }
    }

    private static void listenForRecoveryRequests() throws Exception {

        Process process = new ProcessBuilder(
            "ipfs", "pubsub", "sub", RECOVERY_TOPIC
        ).redirectErrorStream(true).start();

        System.out.println("Líder à escuta de pedidos de recuperação...");

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream()))) {

            String message;

            RecordFile file = new RecordFile("lider-ipfs.txt");

            while ((message = reader.readLine()) != null) {
                System.out.println("Pedido de recuperação recebido: " + message);

                try {
                    long seq = Long.parseLong(message.trim());

                    SensorRecord record = file.findBySeq(seq);

                    if (record != null) {
                        System.out.println("Registo encontrado: " + record.toLine());

                        // Reenviar o registo através do IPFS PubSub
                        publish(record.toLine());

                        System.out.println("Registo reenviado: " + record.toLine());
                    } else {
                        System.out.println("Registo não encontrado: " + seq);
                    }

                } catch (NumberFormatException e) {
                    System.out.println("Pedido de recuperação inválido: " + message);
                }
            }
        }
    }
}

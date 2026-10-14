
package rep01;

import java.io.IOException;
import java.util.Scanner;

public class PubSubLeader {

    private static final String TOPIC = "rep01-temperaturas";
    private static final String DEFAULT_API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) {
        String api = args.length > 0 ? args[0] : DEFAULT_API;
        IpfsPubSub pubsub = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider.txt");

        try (Scanner scanner = new Scanner(System.in)) {
            long seq = file.lastSeq();

            System.out.println("Líder PubSub pronto.");
            System.out.println("Formato: <sensor> <temperatura>");
            System.out.println("Comandos: rajada <n> | sair");

            while (true) {
                System.out.print("> ");

                if (!scanner.hasNextLine()) {
                    break;
                }

                String line = scanner.nextLine().trim();

                if (line.equalsIgnoreCase("sair")) {
                    break;
                }

                String[] parts = line.split("\\s+");

                if (parts.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }

                if (parts[0].equalsIgnoreCase("rajada")) {
                    int n;

                    try {
                        n = Integer.parseInt(parts[1]);

                        if (n <= 0) {
                            System.out.println("O número deve ser positivo.");
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Número de registos inválido.");
                        continue;
                    }

                    long primeiroEnvio = 0;
                    long ultimoEnvio = 0;

                    for (int i = 0; i < n; i++) {
                        double temperatura = 15 + Math.random() * 15;
                        SensorRecord record =
                                SensorRecord.now(++seq, "SIM", temperatura);

                        file.append(record);

                        long instante = System.currentTimeMillis();

                        if (i == 0) {
                            primeiroEnvio = instante;
                        }

                        pubsub.publish(TOPIC, record.toLine());
                        ultimoEnvio = System.currentTimeMillis();
                    }

                    System.out.println("Rajada concluída: " + n + " registos.");
                    System.out.println("Primeiro envio: " + primeiroEnvio);
                    System.out.println("Último envio: " + ultimoEnvio);
                    System.out.println(
                            "Tempo de envio: "
                            + (ultimoEnvio - primeiroEnvio) + " ms"
                    );

                    continue;
                }

                try {
                    double temperatura = Double.parseDouble(parts[1]);

                    SensorRecord record =
                            SensorRecord.now(++seq, parts[0], temperatura);

                    file.append(record);
                    pubsub.publish(TOPIC, record.toLine());

                    System.out.println(
                            "Registado e publicado: " + record.toLine()
                    );

                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida.");
                }
            }

        } catch (IOException | InterruptedException e) {
            System.err.println("Erro no líder PubSub: " + e.getMessage());

            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }
}

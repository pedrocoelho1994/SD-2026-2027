package rep01;

import java.net.*;
import java.nio.charset.StandardCharsets;

public class MulticastReplica {

    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {

        if (args.length != 1) {
            System.out.println("Uso: java rep01.MulticastReplica <id>");
            return;
        }

        String replicaId = args[0];
        RecordFile file = new RecordFile("replica-" + replicaId + ".txt");

        try (MulticastSocket socket = new MulticastSocket(PORT)) {

            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(new InetSocketAddress(group, PORT), null);

            System.out.println("Réplica " + replicaId + " pronta.");
            System.out.println("À espera de mensagens Multicast...");

            byte[] buffer = new byte[2048];

            while (true) {

                DatagramPacket packet =
                    new DatagramPacket(buffer, buffer.length);

                socket.receive(packet);

                String message = new String(
                    packet.getData(),
                    packet.getOffset(),
                    packet.getLength(),
                    StandardCharsets.UTF_8
                );

                try {
                    SensorRecord record = SensorRecord.fromLine(message);

                    long lastSeq = file.lastSeq();

                    if (record.getSeq() > lastSeq) {
                        file.append(record);

                        System.out.println(
                            "Registo recebido e guardado: " + message
                        );
                    } else {
                        System.out.println(
                            "Registo duplicado ou antigo: " + message
                        );
                    }

                } catch (Exception e) {
                    System.out.println(
                        "Mensagem inválida: " + e.getMessage()
                    );
                }
            }

        } catch (Exception e) {
            System.err.println("Erro na réplica: " + e.getMessage());
        }
    }
}
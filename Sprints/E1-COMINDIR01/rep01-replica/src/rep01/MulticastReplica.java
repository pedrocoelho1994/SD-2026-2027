
package rep01;

import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class MulticastReplica {

    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {

        // verifica se foi indicado o identificador da réplica
        if (args.length != 1) {
            System.out.println("Uso: java rep01.MulticastReplica <id>");
            return;
        }

        // cria o ficheiro de registos da réplica
        String replicaId = args[0];
        RecordFile file = new RecordFile("replica-" + replicaId + ".txt");

        // guarda temporariamente os registos recebidos fora de ordem
        // a chave é o número de sequência e o valor é o registo
        TreeMap<Long, SensorRecord> pending = new TreeMap<>();

        // cria o socket multicast na porta definida
        try (MulticastSocket socket = new MulticastSocket(PORT)) {

            // obtém a última sequência guardada no ficheiro
            // fica dentro do try porque lastSeq() pode lançar IOException
            long lastSeq = file.lastSeq();

            // junta-se ao grupo multicast para receber mensagens do líder
            InetAddress group = InetAddress.getByName(GROUP);
            socket.joinGroup(new InetSocketAddress(group, PORT), null);

            System.out.println("Réplica " + replicaId + " pronta.");
            System.out.println("À espera de mensagens Multicast...");
            System.out.println("Última sequência guardada: " + lastSeq);

            // cria o buffer para receber os dados
            byte[] buffer = new byte[2048];

            // mantém a réplica à espera de mensagens
            while (true) {

                DatagramPacket packet =
                    new DatagramPacket(buffer, buffer.length);

                // recebe um pacote UDP do grupo multicast
                socket.receive(packet);

                // converte os dados recebidos numa mensagem de texto
                String message = new String(
                    packet.getData(),
                    packet.getOffset(),
                    packet.getLength(),
                    StandardCharsets.UTF_8
                );

                try {

                    // transforma a mensagem recebida num registo
                    SensorRecord record = SensorRecord.fromLine(message);

                    // obtém o número de sequência do registo recebido
                    long receivedSeq = record.getSeq();

                    // ignora registos já guardados ou que já estão pendentes
                    if (receivedSeq <= lastSeq ||
                        pending.containsKey(receivedSeq)) {

                        System.out.println(
                            "Registo duplicado ou antigo: " + message
                        );

                        continue;
                    }

                    // guarda temporariamente o registo no TreeMap
                    pending.put(receivedSeq, record);

                    // verifica se existem sequências anteriores em falta
                    if (receivedSeq > lastSeq + 1) {

                        System.out.println(
                            "Registo pendente: " + message
                            + " | À espera da sequência " + (lastSeq + 1)
                        );
                    }

                    // processa os registos pela ordem correta
                    // enquanto existir a próxima sequência esperada
                    while (pending.containsKey(lastSeq + 1)) {

                        long nextSeq = lastSeq + 1;

                        // obtém o próximo registo pendente
                        SensorRecord next = pending.get(nextSeq);

                        // guarda o registo no ficheiro da réplica
                        file.append(next);

                        // remove o registo do TreeMap após o guardar
                        pending.remove(nextSeq);

                        // atualiza a última sequência guardada
                        lastSeq = nextSeq;

                        System.out.println(
                            "Registo recebido e guardado: " + next.toLine()
                        );
                    }

                } catch (Exception e) {

                    // apresenta erros ao processar ou guardar registos
                    System.out.println(
                        "Erro ao processar mensagem: " + e.getMessage()
                    );
                }
            }

        } catch (Exception e) {

            // apresenta erros de comunicação ou de inicialização
            System.err.println(
                "Erro na réplica: " + e.getMessage()
            );
        }
    }
}

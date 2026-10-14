package rep01;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.Scanner;

public class MulticastLeader {
    static final String GROUP = "230.0.0.1";
    static final int PORT = 6789;

    public static void main(String[] args) {
        try (MulticastSocket socket = new MulticastSocket();
             Scanner sc = new Scanner(System.in)) {

            InetAddress group = InetAddress.getByName(GROUP);
            socket.setTimeToLive(1);

            RecordFile file = new RecordFile("lider.txt");
            long seq = file.lastSeq();
            Random random = new Random();

            System.out.println("Líder Multicast pronto.");
            System.out.println("Comandos: <sensor> <temperatura> | rajada <n> | sair");

            while (true) {
                System.out.print("> ");

                if (!sc.hasNextLine()) break;

                String line = sc.nextLine().trim();

                if (line.equalsIgnoreCase("sair")) break;

                String[] p = line.split("\\s+");

                if (p.length == 2 && p[0].equalsIgnoreCase("rajada")) {
                    int n;

                    try {
                        n = Integer.parseInt(p[1]);
                        if (n <= 0) {
                            System.out.println("O número deve ser positivo.");
                            continue;
                        }
                    } catch (NumberFormatException e) {
                        System.out.println("Número de registos inválido.");
                        continue;
                    }

                    long inicio = System.currentTimeMillis();
                    System.out.println("Início da rajada: " + java.time.LocalDateTime.now());

                    for (int i = 0; i < n; i++) {
                        double temp = 15 + random.nextDouble() * 15;
                        temp = Math.round(temp * 10.0) / 10.0;

                        SensorRecord r = SensorRecord.now(++seq, "SIM", temp);

                        file.append(r);

                        byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                        socket.send(new DatagramPacket(m, m.length, group, PORT));
                    }

                    long fim = System.currentTimeMillis();

                    System.out.println("Fim da rajada: " + java.time.LocalDateTime.now());
                    System.out.println("Registos enviados: " + n);
                    System.out.println("Duração: " + (fim - inicio) + " ms");
                    continue;
                }

                if (p.length != 2) {
                    System.out.println("Formato inválido.");
                    continue;
                }

                double temp;

                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                SensorRecord r = SensorRecord.now(++seq, p[0], temp);

                file.append(r);

                byte[] m = r.toLine().getBytes(StandardCharsets.UTF_8);
                socket.send(new DatagramPacket(m, m.length, group, PORT));

                System.out.println("Registado e enviado: " + r.toLine());
            }

        } catch (IOException e) {
            System.out.println("IO: " + e.getMessage());
        }
    }
}
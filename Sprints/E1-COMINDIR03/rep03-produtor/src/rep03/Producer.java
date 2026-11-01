package rep03;

import java.util.Random;
import java.util.Properties;
import java.util.Scanner;

public class Producer {
    static final String API = "http://127.0.0.1:5001/api/v0";

    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
        int n = args.length > 0 ? Integer.parseInt(args[0])
                : Integer.parseInt(RuntimeConfig.value(config, "partitions", "3"));
        String api = args.length > 1 ? args[1]
                : RuntimeConfig.value(config, "producer.api", API);
        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub pubsub = new IpfsPubSub(api);
        Random rand = new Random();
        int[] contadores = new int[n];

        System.out.println("Produtor pronto (API: " + api + ", " + n + " particoes).");
        System.out.println("Formato: <sensor> <temperatura> | 'rajada <n> <s>' "
                + "| 'rajada-quente <n> <s>' | 'sair'");

        try (Scanner sc = new Scanner(System.in)) {
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) break;
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) break;
                if (line.isEmpty()) continue;

                boolean quente = line.toLowerCase().startsWith("rajada-quente ");
                if (quente || line.toLowerCase().startsWith("rajada ")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length != 3) {
                        System.out.println("Uso: rajada <n> <s> | rajada-quente <n> <s>");
                        continue;
                    }
                    try {
                        int nLeituras = Integer.parseInt(parts[1]);
                        int s = Integer.parseInt(parts[2]);
                        if (nLeituras < 1 || s < 1) {
                            System.out.println("Valores invalidos.");
                            continue;
                        }
                        long tInicio = System.currentTimeMillis();
                        long tFim = tInicio;
                        for (int i = 0; i < nLeituras; i++) {
                            String sensor = quente
                                    ? sensorQuente(rand, s)
                                    : String.format("S%02d", rand.nextInt(s) + 1);
                            double temp = Math.round((15.0 + 15.0 * rand.nextDouble()) * 10.0) / 10.0;
                            publicar(pubsub, partitioner, contadores, sensor, temp);
                            tFim = System.currentTimeMillis();
                        }
                        System.out.println("Rajada concluida: " + nLeituras + " leituras.");
                        for (int k = 0; k < n; k++) {
                            System.out.println("p" + k + ": " + contadores[k] + " leituras");
                        }
                        System.out.println("Primeira: " + tInicio + " ms | ultima: " + tFim
                                + " ms | Duracao: " + (tFim - tInicio) + " ms");
                    } catch (NumberFormatException e) {
                        System.out.println("rajada: n e s tem de ser inteiros.");
                    }
                    continue;
                }

                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato invalido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura invalida: " + p[1]);
                    continue;
                }
                publicar(pubsub, partitioner, contadores, p[0], temp);
            }
        } catch (Exception e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }

    static void publicar(IpfsPubSub ps, Partitioner pt, int[] contadores,
                     String sensor, double temp) throws Exception {
            int k = pt.of(sensor);
            VectorClock zero = new VectorClock(pt.count());   // L=0, V=(0,…,0) — produtor não observa registos
            String msg = Wire.in(sensor, temp, 0, zero);
                ps.publish(Wire.topicIn(k), msg);
                contadores[k]++;
            System.out.println("Publicado: " + msg + " -> p" + k);
        }

    static String sensorQuente(Random rand, int s) {
        if (s >= 2 && rand.nextDouble() >= 0.8) {
            return String.format("S%02d", rand.nextInt(s - 1) + 2);
        }
        return "S01";
    }
}
package rep03;

import java.util.Random;
import java.util.Properties;
import java.util.Scanner;

public class Producer {
    static final String API = "http://127.0.0.1:5001/api/v0"; // API por omissao

    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
        // numero de particoes e endereco da API: Producer <N> [api];
        // sem argumentos, os valores sao lidos de runtime.properties
        int n = args.length > 0 ? Integer.parseInt(args[0])
                : Integer.parseInt(RuntimeConfig.value(config, "partitions", "3"));
        String api = args.length > 1 ? args[1] : RuntimeConfig.value(config, "producer.api", API);
        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub pubsub = new IpfsPubSub(api);
        Random rand = new Random();              // sensores e temperaturas aleatorias
        int[] contadores = new int[n];           // leituras publicadas por particao

        System.out.println("Produtor pronto (API: " + api + ", " + n + " particoes).");
        System.out.println("Formato: <sensor> <temperatura> | 'rajada <n> <s>' | 'rajada-quente <n> <s>' | 'sair'");

        try (Scanner sc = new Scanner(System.in)) {
            // ciclo de leitura de comandos: bloqueia a espera de uma linha
            while (true) {
                System.out.print("> ");
                if (!sc.hasNextLine()) {
                    break;
                }
                String line = sc.nextLine().trim();
                if (line.equalsIgnoreCase("sair")) {
                    break;
                }
                if (line.isEmpty()) {
                    continue;         // ignora linhas vazias
                }

                // rajada <n> <s>: publica n leituras, o mais depressa possivel, com sensores aleatorios entre S01 e S<s>
                // rajada-quente <n> <s>: igual, mas 80 % das leituras vem de S01 (chave quente)
                boolean quente = line.toLowerCase().startsWith("rajada-quente ");
                if (quente || line.toLowerCase().startsWith("rajada ")) {
                    String[] parts = line.split("\\s+");
                    if (parts.length != 3) {
                        System.out.println("Uso: rajada <n> <s> | rajada-quente <n> <s>");
                        continue;
                    }
                    try {
                        int nLeituras = Integer.parseInt(parts[1]); // numero de leituras
                        int s = Integer.parseInt(parts[2]); // sensores de S01 a S<s>
                        if (nLeituras < 1 || s < 1) {
                            System.out.println("Valores invalidos.");
                            continue;
                        }

                        long tInicio = System.currentTimeMillis(); // instante da 1a publicacao
                        long tFim = tInicio;
                        for (int i = 0; i < nLeituras; i++) {
                            String sensor = quente
                                    ? sensorQuente(rand, s)
                                    : String.format("S%02d", rand.nextInt(s) + 1); // sensor aleaterio
                            double temp = Math.round((15.0 + 15.0 * rand.nextDouble()) * 10.0) / 10.0; // temperatura aleatoria entre 15 e 30
                            publicar(pubsub, partitioner, contadores, sensor, temp);
                            tFim = System.currentTimeMillis(); // instante da ultima publicacao
                        }
                        System.out.println("Rajada concluida: " + nLeituras + " leituras.");
                        for (int k = 0; k < n; k++) {
                            System.out.println("p" + k + ": " + contadores[k] + " leituras");
                        }
                        System.out.println("Primeira: " + tInicio + " ms | ultima: " + tFim + " ms | Duracao: " + (tFim - tInicio) + " ms");
                    } catch (NumberFormatException e) {
                        System.out.println("rajada: n e s tem de ser inteiros.");
                    }
                    continue;
                }

                // publica uma leitura
                String[] p = line.split("\\s+");
                if (p.length != 2) {
                    System.out.println("Formato invalido.");
                    continue;
                }
                double temp;
                try {
                    temp = Double.parseDouble(p[1]); // converte o texto em numero
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura invalida: " + p[1]);
                    continue;
                }
                publicar(pubsub, partitioner, contadores, p[0], temp); // publica IN no topico de entrada da particao do sensor
            }
        } catch (Exception e) {
            System.out.println("Erro: " + e.getMessage());
        }
    }

    // publica a leitura no topico de entrada da particao do sensor e assinala o destino
    static void publicar(IpfsPubSub ps, Partitioner pt, int[] contadores, String sensor, double temp) throws Exception {
        int k = pt.of(sensor);
        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp));
        contadores[k]++;
        System.out.println("Publicado: " + Wire.in(sensor, temp) + " -> p" + k);
    }

    // sensor de uma leitura de rajada-quente: 80 % vem de S01, o resto de S02 a S<s>
    static String sensorQuente(Random rand, int s) {
        if (s >= 2 && rand.nextDouble() >= 0.8) {
            return String.format("S%02d", rand.nextInt(s - 1) + 2);
        }
        return "S01";
    }
}

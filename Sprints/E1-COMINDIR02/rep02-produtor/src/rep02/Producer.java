package rep02;

import java.util.Scanner;

public class Producer {

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.out.println("Uso: Producer <N> [api]");
            return;
        }

        int n = Integer.parseInt(args[0]);
        String api = args.length > 1 ? args[1] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);
        Scanner sc = new Scanner(System.in);
        

        System.out.println("Produtor pronto. N=" + n + "  API: " + api);
        System.out.println("Comandos: <sensor> <temperatura> | rajada <n> <s> | rajada-quente <n> <s> | sair");

        while (sc.hasNextLine()) {
            String line = sc.nextLine().trim();
            if (line.isEmpty()) continue;
            if (line.equalsIgnoreCase("sair")) break;

            String[] p = line.split("\\s+");

            // ---- rajada <n> <s>  e  rajada-quente <n> <s> ----
            if (p.length == 3 && (p[0].equalsIgnoreCase("rajada")
                               || p[0].equalsIgnoreCase("rajada-quente"))) {

                boolean quente = p[0].equalsIgnoreCase("rajada-quente");
                int total, sMax;
                try {
                    total = Integer.parseInt(p[1]);
                    sMax  = Integer.parseInt(p[2]);
                    if (total <= 0 || sMax <= 0) {
                        System.out.println("n e s devem ser positivos.");
                        continue;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Número inválido.");
                    continue;
                }

                int[] contagem = new int[n];
                long primeira = 0, ultima = 0;

                try {
                    for (int i = 0; i < total; i++) {
                        String sensor;
                        if (quente && Math.random() < 0.8) {
                            sensor = "S01";
                        } else if (quente) {
                            sensor = String.format("S%02d",
                                    2 + (int) (Math.random() * (sMax - 1)));
                        } else {
                            sensor = String.format("S%02d",
                                    1 + (int) (Math.random() * sMax));
                        }
                        double temp = 15 + Math.random() * 15;
                        int k = partitioner.of(sensor);
                        contagem[k]++;

                        long now = System.currentTimeMillis();
                        if (i == 0) primeira = now;

                        ps.publish(Wire.topicIn(k), Wire.in(sensor, temp));
                        ultima = System.currentTimeMillis();

                        System.out.println("→ p" + k + "  " + sensor);
                    }

                    System.out.println("Rajada concluída: " + total + " leituras.");
                    System.out.println("primeira=" + primeira);
                    System.out.println("ultima=" + ultima);
                    System.out.println("duração=" + (ultima - primeira) + " ms");
                    for (int k = 0; k < n; k++) {
                        System.out.println("p" + k + ": " + contagem[k] + " leituras");
                    }

                } catch (Exception e) {
                    System.err.println("Falha na rajada: "
                            + e.getClass().getName() + " — " + e.getMessage());
                }
                continue;
            }

            if (p.length == 2) {
                String sensor = p[0];
                double temp;
                try {
                    temp = Double.parseDouble(p[1]);
                } catch (NumberFormatException e) {
                    System.out.println("Temperatura inválida: " + p[1]);
                    continue;
                }

                int k = partitioner.of(sensor);
                try {
                    ps.publish(Wire.topicIn(k), Wire.in(sensor, temp));
                    System.out.println("→ p" + k + "  " + Wire.in(sensor, temp));
                } catch (Exception e) {
                    System.err.println("Falha ao publicar: "
                            + e.getClass().getName() + " — " + e.getMessage());
                }
                continue;
            }

            System.out.println("Formato inválido.");
        }
        sc.close();
    }
}
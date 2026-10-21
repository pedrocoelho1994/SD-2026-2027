package rep02;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;

public class PartitionReplica {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionReplica <id> <partições> [portoApi]");
            System.out.println("  Parte A:  PartitionReplica 1 0");
            System.out.println("  Parte B:  PartitionReplica 1 0,1");
            return;
        }

        int id = Integer.parseInt(args[0]);
        String[] parts = args[1].split(",");
        String portoApi = args.length > 2 ? args[2] : "5001";
        String api = "http://127.0.0.1:" + portoApi + "/api/v0";

        // propriedades de sistema para testes controlados
        double perda = Double.parseDouble(System.getProperty("perda", "0"));
        Long descartar = System.getProperty("descartar") != null
                ? Long.parseLong(System.getProperty("descartar")) : null;

        IpfsPubSub ps = new IpfsPubSub(api);

        System.out.println("Réplica " + id + " pronta. Partições: " + args[1]);
        System.out.println("API: " + api);
        if (perda > 0) System.out.println("Simulação: perda=" + perda);
        if (descartar != null) System.out.println("Simulação: descartar=" + descartar);

        // uma thread por partição — cada uma com o seu estado independente
        for (String p : parts) {
            int k = Integer.parseInt(p.trim());
            Thread t = new Thread(() -> runPartition(id, k, ps, perda, descartar));
            t.setName("replica-" + id + "-p" + k);
            t.setDaemon(false);
            t.start();
        }

        // a main thread fica à espera que as threads das partições terminem
        Thread.currentThread().join();
    }

    /**
     * Corre a subscrição e a recuperação de uma partição.
     * Todo o estado (ficheiro, último seq, retidos, endereço do líder) é local a esta thread.
     */
    static void runPartition(int id, int k, IpfsPubSub ps,
                              double perda, Long descartar) {
        try {
            RecordFile file = new RecordFile("replica-" + id + "-p" + k + ".txt");
            long[] last = { file.lastSeq() };                   // lido uma vez
            TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
            String[] host = { null };                           // aprendido de cada REG
            int[] port = { -1 };
            boolean[] jaDescartou = { false };

            System.out.println("[p" + k + "] ficheiro replica-" + id + "-p" + k
                    + ".txt (último seq = " + last[0] + ")");

            ps.subscribe(Wire.topicData(k), msg -> {
                try {
                    // 1. extrair endereço/porto do líder e o registo
                    String[] rp = Wire.regParts(msg);
                    host[0] = rp[0];
                    port[0] = Integer.parseInt(rp[1]);
                    SensorRecord r = SensorRecord.fromLine(rp[2]);

                    // 2. simulações — só para mensagens do PubSub
                    if (descartar != null && !jaDescartou[0] && r.getSeq() == descartar) {
                        jaDescartou[0] = true;
                        System.out.println("[p" + k + "] DESCARTADO (simulação): " + r.getSeq());
                        return;
                    }
                    if (perda > 0 && Math.random() < perda) {
                        System.out.println("[p" + k + "] DESCARTADO (simulação): " + r.getSeq());
                        return;
                    }

                    // 3. aplicar a regra
                    processRecord(k, r, file, last, retidos);

                    // 4. gatilho de recuperação
                    if (retidos.size() > Wire.LIMITE_RETIDOS && host[0] != null) {
                        recuperar(k, file, last, retidos, host[0], port[0]);
                    }

                } catch (IllegalArgumentException e) {
                    System.out.println("[p" + k + "] REJEITADO: " + msg);
                } catch (Exception e) {
                    System.err.println("[p" + k + "] erro: "
                            + e.getClass().getName() + " — " + e.getMessage());
                }
            });

        } catch (Exception e) {
            System.err.println("[p" + k + "] terminou: "
                    + e.getClass().getName() + " — " + e.getMessage());
        }
    }

    /**
     * A regra de aplicação da COMINDIR01, generalizada:
     *   - seq == último+1  → escreve e aplica a cascata de retidos
     *   - seq <= último ou já retido → duplicado, ignora
     *   - seq > último+1   → retém em memória
     */
    static void processRecord(int k, SensorRecord r, RecordFile file,
                               long[] last, TreeMap<Long, SensorRecord> retidos)
            throws IOException {
        long received = r.getSeq();
        long expected = last[0] + 1;

        if (received <= last[0] || retidos.containsKey(received)) {
            System.out.println("[p" + k + "] DUPLICADO: " + received);

        } else if (received > expected) {
            retidos.put(received, r);
            System.out.println("[p" + k + "] EM ESPERA: " + received
                    + " (falta o registo " + expected + ")");

        } else {
            file.append(r);
            last[0] = received;
            System.out.println("[p" + k + "] APLICADO: " + r.toLine());

            // cascata: aplicar os retidos que passaram a estar em sequência
            long next = received + 1;
            while (retidos.containsKey(next)) {
                SensorRecord n = retidos.remove(next);
                file.append(n);
                last[0] = next;
                System.out.println("[p" + k + "] APLICADO DA MEMÓRIA: " + n.toLine());
                next++;
            }
        }
    }

    /**
     * Recuperação por TCP: pede ao líder o intervalo em falta.
     * Bloqueia a thread de subscrição desta partição durante a recuperação.
     */
    static void recuperar(int k, RecordFile file, long[] last,
                           TreeMap<Long, SensorRecord> retidos,
                           String host, int port) {
        long de = last[0] + 1;
        long ate = retidos.firstKey() - 1;

        if (de > ate) {
            System.out.println("[p" + k + "] RECUPERAÇÃO FALHOU: intervalo inválido "
                    + de + ".." + ate);
            return;
        }

        System.out.println("[p" + k + "] RECUPERAÇÃO: " + de + ".." + ate
                + " pedida a " + host + ":" + port);

        try (Socket s = new Socket()) {
            s.connect(new InetSocketAddress(host, port), Wire.T_TCP_MS);
            s.setSoTimeout(Wire.T_TCP_MS);

            PrintWriter out = new PrintWriter(
                    new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
            BufferedReader in = new BufferedReader(
                    new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));

            out.println("PEDIDO " + de + " " + ate);

            String line;
            boolean fim = false;
            while ((line = in.readLine()) != null) {
                if (line.equals("FIM")) {
                    fim = true;
                    break;
                }
                if (line.startsWith("ERRO")) {
                    System.out.println("[p" + k + "] RECUPERAÇÃO FALHOU: " + line);
                    return;
                }
                SensorRecord r = SensorRecord.fromLine(line);
                System.out.println("[p" + k + "] RECUPERADO: " + r.getSeq());
                processRecord(k, r, file, last, retidos);   // mesma regra, com cascata
            }

            if (!fim) {
                System.out.println("[p" + k
                        + "] RECUPERAÇÃO FALHOU: resposta interrompida antes de FIM");
            }

        } catch (Exception e) {
            System.out.println("[p" + k + "] RECUPERAÇÃO FALHOU: "
                    + e.getClass().getName() + " — " + e.getMessage());
        }
    }
}
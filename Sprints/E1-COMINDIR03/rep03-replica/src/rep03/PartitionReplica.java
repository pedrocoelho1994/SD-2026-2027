package rep03;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;

public class PartitionReplica {

    private static double perda;
    private static long descartar;
    private static final AtomicBoolean JA_DESCARTADO = new AtomicBoolean(false);
    private static int n = 3;

    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
        n = Integer.parseInt(RuntimeConfig.value(config, "partitions", "3"));
        try {
            perda = Double.parseDouble(System.getProperty("perda",
                    RuntimeConfig.value(config, "replica.perda", "0")));
            descartar = Long.parseLong(System.getProperty("descartar",
                    RuntimeConfig.value(config, "replica.descartar", "-1")));
        } catch (NumberFormatException e) {
            System.out.println("Propriedade de simulacao invalida: " + e.getMessage());
            return;
        }

        String apiPort = args.length > 2 ? args[2]
                : RuntimeConfig.value(config, "replica.api.port", "5001");
        String api = "http://127.0.0.1:" + apiPort + "/api/v0";

        String id;
        String partes;
        int[] ks;
        if (args.length == 0) {
            Atribuicao a = autoAtribuicao();
            id = a.id();
            ks = a.ks();
            partes = a.partes();
            System.out.println("atribuicao automatica: replica " + id
                    + " com as particoes " + partes);
        } else {
            id = args[0];
            partes = args.length > 1 ? args[1] : "0";
            try {
                String[] campos = partes.split(",");
                ks = new int[campos.length];
                for (int i = 0; i < campos.length; i++) {
                    ks[i] = Integer.parseInt(campos[i].trim());
                }
            } catch (NumberFormatException e) {
                System.out.println("Uso: PartitionReplica [id] [particoes] [portoApi]");
                System.out.println("particoes mal formadas: " + partes);
                return;
            }
        }

        System.out.println("replica " + id + " ligada a " + api + " com as particoes "
                + partes + " (perda=" + perda + ", descartar="
                + (descartar < 0 ? "nenhum" : descartar) + ")");

        Thread[] threads = new Thread[ks.length];
        for (int i = 0; i < ks.length; i++) {
            Particao p = new Particao(api, id, ks[i]);
            threads[i] = new Thread(p, "particao-" + ks[i]);
            threads[i].start();
        }
        for (Thread t : threads) {
            t.join();
        }
    }

    private static final List<FileChannel> TRANCAS = new ArrayList<>();

    private static final class Atribuicao {
        private final String id;
        private final int[] ks;
        private final String partes;

        Atribuicao(String id, int[] ks, String partes) {
            this.id = id;
            this.ks = ks;
            this.partes = partes;
        }

        String id() { return id; }
        int[] ks() { return ks; }
        String partes() { return partes; }
    }

    static Atribuicao autoAtribuicao() throws IOException {
        for (int c = 0; c < 100; c++) {
            int[] ks = { c % 3, (c + 1) % 3 };
            List<FileChannel> canais = new ArrayList<>();
            try {
                for (int k : ks) {
                    FileChannel ch = FileChannel.open(
                            Paths.get("replica-" + c + "-p" + k + ".lock"),
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    if (ch.tryLock() == null) {
                        ch.close();
                        throw new IllegalStateException("id ocupado");
                    }
                    canais.add(ch);
                }
                TRANCAS.addAll(canais);
                return new Atribuicao(Integer.toString(c), ks, ks[0] + "," + ks[1]);
            } catch (IOException | IllegalStateException e) {
                for (FileChannel ch : canais) {
                    ch.close();
                }
            }
        }
        throw new IOException("sem ids livres (0..99)");
    }

    private static class Particao implements Runnable {
        private final String api;
        private final int k;
        private final RecordFile file;
        private long ultimo;
        private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        private String hostLider;
        private int portoLider;

        Particao(String api, String id, int k) throws IOException {
            this.api = api;
            this.k = k;
            this.file = new RecordFile("replica-" + id + "-p" + k + ".txt", n);
            this.ultimo = file.lastSeq();
        }

        @Override
        public void run() {
            System.out.println("particao " + k + ": ultimo seq em ficheiro: " + ultimo);
            try {
                new IpfsPubSub(api).subscribe(Wire.topicData(k), this::onReg);
            } catch (Exception e) {
                System.out.println("subscricao da particao " + k + " terminada: " + e.getMessage());
            }
        }

        private void onReg(String msg) {
            try {
                String[] f = Wire.regParts(msg);
                SensorRecord r = SensorRecord.fromLine(f[2], n);  // <-- passa n

                if (r.getSeq() == descartar && JA_DESCARTADO.compareAndSet(false, true)) {
                    System.out.println("DESCARTADO (simulacao): " + r.getSeq());
                    return;
                }
                if (ThreadLocalRandom.current().nextDouble() < perda) {
                    System.out.println("DESCARTADO (simulacao): " + r.getSeq());
                    return;
                }

                hostLider = f[0];
                portoLider = Integer.parseInt(f[1]);
                aplicar(r, false);
                if (retidos.size() > Wire.LIMITE_RETIDOS) {
                    recuperar();
                }
            } catch (IllegalArgumentException e) {
                System.out.println("REJEITADO: " + msg.trim());
            } catch (IOException e) {
                System.out.println("Erro ao escrever no ficheiro: " + e.getMessage());
            }
        }

        private void aplicar(SensorRecord r, boolean recuperado) throws IOException {
            long seq = r.getSeq();
            if (seq <= ultimo || retidos.containsKey(seq)) {
                System.out.println("DUPLICADO: " + seq);
            } else if (seq == ultimo + 1) {
                file.append(r);
                ultimo = seq;
                System.out.println(recuperado ? "RECUPERADO: " + seq : "Aplicado: " + r.toLine());
                escreverEmSequencia();
            } else {
                retidos.put(seq, r);
                System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ")");
            }
        }

        private void escreverEmSequencia() throws IOException {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                SensorRecord r = retidos.remove(retidos.firstKey());
                file.append(r);
                ultimo = r.getSeq();
                System.out.println("Aplicado (que estava retido): " + r.toLine());
            }
        }

        private void recuperar() {
            long de = ultimo + 1;
            long ate = retidos.isEmpty() ? de - 1 : retidos.firstKey() - 1;
            if (de > ate || hostLider == null) {
                return;
            }
            System.out.println("RECUPERACAO: " + de + ".." + ate
                    + " pedida a " + hostLider + ":" + portoLider);
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(hostLider, portoLider), Wire.T_TCP_MS);
                s.setSoTimeout(Wire.T_TCP_MS);
                PrintWriter out = new PrintWriter(new OutputStreamWriter(
                        s.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        s.getInputStream(), StandardCharsets.UTF_8));

                out.println("PEDIDO " + de + " " + ate);

                String linha;
                while ((linha = in.readLine()) != null && !linha.equals("FIM")) {
                    if (linha.startsWith("ERRO")) {
                        throw new IOException(linha.substring("ERRO".length()).trim());
                    }
                    aplicar(SensorRecord.fromLine(linha, n), true);  // <-- passa n
                }
                if (linha == null) {
                    throw new IOException("ligacao fechada antes de FIM");
                }
                escreverEmSequencia();
            } catch (IOException | RuntimeException e) {
                System.out.println("RECUPERACAO FALHOU: " + e.getMessage());
            }
        }
    }
}
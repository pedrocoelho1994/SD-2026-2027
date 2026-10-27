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

    // propriedades da simulacao de perdas (-Dperda, -Ddescartar ou replica.perda,
    // replica.descartar em runtime.properties), aplicadas exclusivamente as
    // mensagens recebidas pelo PubSub
    private static double perda;
    private static long descartar;
    private static final AtomicBoolean JA_DESCARTADO = new AtomicBoolean(false);

    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
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
            // sem argumentos: id e particoes por atribuicao automatica - a replica j guarda
            // as particoes j e j+1 (modulo 3); tres arranques dao (0,1), (1,2) e (2,0),
            // com cada particao em exatamente duas replicas (R = 2, Parte B)
            Atribuicao a = autoAtribuicao();
            id = a.id();
            ks = a.ks();
            partes = a.partes();
            System.out.println("atribuicao automatica: replica " + id + " com as particoes " + partes);
        } else {
            id = args[0];
            partes = args.length > 1 ? args[1] : "0"; // sem lista: particao 0
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

        System.out.println("replica " + id + " ligada a " + api + " com as particoes " + partes
                + " (perda=" + perda + ", descartar=" + (descartar < 0 ? "nenhum" : descartar) + ")");

        // uma thread por particao: a subscricao bloqueia e nao termina
        Thread[] threads = new Thread[ks.length];
        for (int i = 0; i < ks.length; i++) {
            Particao p = new Particao(api, id, ks[i]);
            threads[i] = new Thread(p, "particao-" + ks[i]);
            threads[i].start();
        }
        for (Thread t : threads) { // a thread principal aguarda a conclusao das subscricoes
            t.join();
        }
    }

    // canais das trancas de id automatico: mantem-se abertos ate o processo terminar
    private static final List<FileChannel> TRANCAS = new ArrayList<>();

    // resultado da atribuicao automatica: id da replica e as particoes que lhe ficam
    private record Atribuicao(String id, int[] ks, String partes) { }

    // atribuicao automatica de id e particoes: a replica j guarda as particoes j e
    // j+1, modulo 3; o primeiro id (0, 1, 2, ...) cujos ficheiros replica-<id>-p<k>.lock
    // nao estejam trancados por outra replica ainda viva; a tranca e libertada pelo SO
    // quando o processo morre
    static Atribuicao autoAtribuicao() throws IOException {
        for (int n = 0; n < 100; n++) { // limite de seguranca: 100 replicas vivas chega
            int[] ks = { n % 3, (n + 1) % 3 }; // tres particoes, fator de replicacao 2
            List<FileChannel> canais = new ArrayList<>();
            try {
                for (int k : ks) {
                    FileChannel ch = FileChannel.open(Paths.get("replica-" + n + "-p" + k + ".lock"),
                            StandardOpenOption.CREATE, StandardOpenOption.WRITE);
                    if (ch.tryLock() == null) { // outra replica viva tem este id
                        ch.close();
                        throw new IllegalStateException("id ocupado");
                    }
                    canais.add(ch);
                }
                TRANCAS.addAll(canais); // so aqui e que o id fica tomado
                return new Atribuicao(Integer.toString(n), ks, ks[0] + "," + ks[1]);
            } catch (IOException | IllegalStateException e) {
                for (FileChannel ch : canais) { // liberta as trancas obtidas e tenta o id seguinte
                    ch.close();
                }
            }
        }
        throw new IOException("sem ids livres (0..99)");
    }

    // estado e logica de uma particao subscrita (uma instancia por particao)
    private static class Particao implements Runnable {
        private final String api;
        private final int k;
        private final RecordFile file;
        private long ultimo;                                       // ultimo seq escrito
        private final TreeMap<Long, SensorRecord> retidos = new TreeMap<>();
        private String hostLider;                                  // atualizados a cada REG recebida
        private int portoLider;

        Particao(String api, String id, int k) throws IOException {
            this.api = api;
            this.k = k;
            this.file = new RecordFile("replica-" + id + "-p" + k + ".txt");
            this.ultimo = file.lastSeq();
        }

        @Override
        public void run() {
            System.out.println("particao " + k + ": ultimo seq em ficheiro: " + ultimo);
            try {
                new IpfsPubSub(api).subscribe(Wire.topicData(k), this::onReg);
            } catch (Exception e) { // daemon indisponivel, interrupcao...
                System.out.println("subscricao da particao " + k + " terminada: " + e.getMessage());
            }
        }

        // rececao de uma mensagem REG pelo PubSub
        private void onReg(String msg) {
            try {
                String[] f = Wire.regParts(msg); // { endereco, porto, linha do registo }
                SensorRecord r = SensorRecord.fromLine(f[2]);

                if (r.getSeq() == descartar && JA_DESCARTADO.compareAndSet(false, true)) {
                    System.out.println("DESCARTADO (simulacao): " + r.getSeq()); // apenas a primeira rececao
                    return;
                }
                if (ThreadLocalRandom.current().nextDouble() < perda) {
                    System.out.println("DESCARTADO (simulacao): " + r.getSeq());
                    return;
                }

                hostLider = f[0];
                portoLider = Integer.parseInt(f[1]);
                aplicar(r, false);
                if (retidos.size() > Wire.LIMITE_RETIDOS) { // a condicao persiste se a recuperacao falhar
                    recuperar(); // na propria thread de subscricao
                }
            } catch (IllegalArgumentException e) { // REG, registo ou porto mal formado
                System.out.println("REJEITADO: " + msg.trim());
            } catch (IOException e) {
                System.out.println("Erro ao escrever no ficheiro: " + e.getMessage());
            }
        }

        // regra de aplicacao da COMINDIR01, comum a rececao por PubSub e a recuperacao
        private void aplicar(SensorRecord r, boolean recuperado) throws IOException {
            long seq = r.getSeq();
            if (seq <= ultimo || retidos.containsKey(seq)) { // ja escrito ou retido
                System.out.println("DUPLICADO: " + seq);
            } else if (seq == ultimo + 1) {
                file.append(r);
                ultimo = seq;
                System.out.println(recuperado ? "RECUPERADO: " + seq : "Aplicado: " + r.toLine());
                escreverEmSequencia();
            } else { // registo antecipado: fica retido em memoria
                retidos.put(seq, r);
                System.out.println("EM ESPERA: " + seq + " (falta o registo " + (ultimo + 1) + ")");
            }
        }

        // escreve os retidos que passaram a estar em sequencia
        private void escreverEmSequencia() throws IOException {
            while (!retidos.isEmpty() && retidos.firstKey() == ultimo + 1) {
                SensorRecord r = retidos.remove(retidos.firstKey());
                file.append(r);
                ultimo = r.getSeq();
                System.out.println("Aplicado (que estava retido): " + r.toLine());
            }
        }

        // ligacao TCP ao lider: PEDIDO <de> <ate> e leitura dos registos ate FIM
        private void recuperar() {
            long de = ultimo + 1;
            long ate = retidos.isEmpty() ? de - 1 : retidos.firstKey() - 1;
            if (de > ate || hostLider == null) { // nada a pedir ou lider ainda desconhecido
                return;
            }
            System.out.println("RECUPERACAO: " + de + ".." + ate + " pedida a " + hostLider + ":" + portoLider);
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(hostLider, portoLider), Wire.T_TCP_MS); // bloqueia ate a ligacao ser aceite
                s.setSoTimeout(Wire.T_TCP_MS); // readLine deixa de bloquear indefinidamente
                PrintWriter out = new PrintWriter(new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true);
                BufferedReader in = new BufferedReader(new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));

                out.println("PEDIDO " + de + " " + ate);

                String linha;
                while ((linha = in.readLine()) != null && !linha.equals("FIM")) {
                    if (linha.startsWith("ERRO")) { // pedido recusado pelo lider
                        throw new IOException(linha.substring("ERRO".length()).trim());
                    }
                    aplicar(SensorRecord.fromLine(linha), true);
                }
                if (linha == null) { // a ligacao fechou antes de FIM
                    throw new IOException("ligacao fechada antes de FIM");
                }
                escreverEmSequencia();
            } catch (IOException | RuntimeException e) { // recusa, tempo esgotado, registo mal formado...
                System.out.println("RECUPERACAO FALHOU: " + e.getMessage());
            }
        }
    }
}

package rep03;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

public class PartitionLeader {
    private static final Object LOCK = new Object();

    public static void main(String[] args) throws Exception {
        Properties config = RuntimeConfig.load();
        int n = args.length > 1 ? Integer.parseInt(args[1])
                : Integer.parseInt(RuntimeConfig.value(config, "partitions", "3"));
        Partitioner partitioner = new Partitioner(n);
        String endereco = args.length > 2 ? args[2]
                : RuntimeConfig.value(config, "leader.host", "127.0.0.1");
        String api = args.length > 3 ? args[3]
                : RuntimeConfig.value(config, "leader.api", "http://127.0.0.1:5002/api/v0");

        ServerSocket server = null;
        final int k;

        try {
            if (args.length > 0) {
                k = Integer.parseInt(args[0]);
                server = new ServerSocket(Wire.PORTO_BASE + k);
            } else {
                int livre = -1;
                ServerSocket s = null;
                for (int i = 0; i < n && s == null; i++) {
                    try {
                        s = new ServerSocket(Wire.PORTO_BASE + i);
                        livre = i;
                    } catch (IOException e) {
                        // porto ocupado: ja ha lider desta particao
                    }
                }
                if (s == null) {
                    System.out.println("ja ha lider nas " + n + " particoes (portos "
                            + Wire.PORTO_BASE + ".." + (Wire.PORTO_BASE + n - 1) + ").");
                    return;
                }
                server = s;
                k = livre;
                System.out.println("particao atribuida automaticamente: p" + k);
            }

            final int porto = Wire.PORTO_BASE + k;
            IpfsPubSub ps = new IpfsPubSub(api);
            RecordFile file = new RecordFile("lider-p" + k + ".txt", n);

            // --- inicializacao a partir da ultima linha do ficheiro (CA2) ---
            SensorRecord lastRecord = file.last();
            final AtomicLong lamport = new AtomicLong(lastRecord == null ? 0 : lastRecord.lamport);
            final VectorClock vector = (lastRecord == null)
                    ? new VectorClock(n)
                    : lastRecord.vector.copy();
            AtomicLong seq = new AtomicLong(lastRecord == null ? 0 : lastRecord.getSeq());

            // ENTRADA (PubSub, thread secundaria)
            Thread entrada = new Thread(() -> {
                try {
                    ps.subscribe(Wire.topicIn(k), msg -> {
                        try {
                            String[] f = msg.trim().split(";");
                            // formato: IN;<sensor>;<temperatura>;<L>;<V>
                            if (f.length != 5 || !f[0].equals("IN") || f[1].isBlank()) {
                                throw new IllegalArgumentException("IN mal formado: " + msg);
                            }
                            int pj = partitioner.of(f[1]);
                            if (pj != k) {
                                System.out.println("PARTICAO ERRADA: " + f[1] + " pertence a p" + pj);
                                return;
                            }
                            double temp = Double.parseDouble(f[2]);
                            long Lmsg = Long.parseLong(f[3]);
                            VectorClock Vmsg = VectorClock.parse(f[4], n);

                            SensorRecord r;
                            long Lfinal;
                            synchronized (LOCK) {
                                // (1) regra de Lamport
                                Lfinal = Math.max(lamport.get(), Lmsg) + 1;
                                // (2) regra vetorial: merge + tick (sobre copia para poder reverter)
                                VectorClock novoVetor = vector.copy();
                                novoVetor.merge(Vmsg);
                                novoVetor.tick(k);
                                // (3) atribuicao de seq
                                long s = seq.get() + 1;
                                // (4) verificacao V[k] == seq (antes de publicar qualquer alteracao)
                                if (novoVetor.get(k) != s) {
                                    throw new IllegalStateException(
                                        "V[" + k + "]=" + novoVetor.get(k) + " != seq=" + s);
                                }
                                // (5) commit: aplicar em todos os campos so depois da verificacao
                                lamport.set(Lfinal);
                                vector.merge(Vmsg);
                                vector.tick(k);
                                seq.set(s);
                                r = SensorRecord.now(s, f[1], temp, Lfinal, vector.copy());
                                file.append(r);
                                }
                            System.out.println("EVENTO: seq=" + r.getSeq()
                                    + " L=" + r.lamport + " V=" + r.vector);
                            ps.publish(Wire.topicData(k), Wire.reg(endereco, porto, r));
                        } catch (Exception e) {
                            System.out.println("REJEITADO: " + msg + " (" + e.getMessage() + ")");
                        }
                    });
                } catch (Exception e) {
                    System.out.println("Entrada terminada: " + e.getMessage());
                }
            });
            entrada.setDaemon(true);
            entrada.start();

            // RECUPERACAO (TCP, thread principal)
            System.out.println("lider da particao " + k + " pronto (recuperacao em "
                    + endereco + ":" + porto + ")");

            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(new InputStreamReader(
                             s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(new OutputStreamWriter(
                             s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();
                    long de = 0, ate = 0;

                    try {
                        if (pedido == null) {
                            throw new IllegalArgumentException("sem pedido");
                        }
                        String[] f = pedido.trim().split("\\s+");
                        if (f.length != 3 || !f[0].equals("PEDIDO")) {
                            throw new IllegalArgumentException("pedido mal formado: " + pedido);
                        }
                        try {
                            de = Long.parseLong(f[1]);
                            ate = Long.parseLong(f[2]);
                        } catch (NumberFormatException e) {
                            throw new IllegalArgumentException("numeros invalidos: " + pedido);
                        }
                        if (de > ate) {
                            throw new IllegalArgumentException("intervalo vazio: " + de + " > " + ate);
                        }
                    } catch (IllegalArgumentException e) {
                        out.println("ERRO " + e.getMessage());
                        continue;
                    }

                    long ultimo;
                    synchronized (LOCK) {
                        ultimo = seq.get();
                    }

                    long limite = Math.min(ate, ultimo);
                    List<SensorRecord> registos = file.range(de, limite);
                    for (SensorRecord r : registos) {
                        out.println(r.toLine());
                    }
                    out.println("FIM");
                    System.out.println("RECUPERACAO: " + de + ".." + limite + " para "
                            + s.getRemoteSocketAddress() + " (" + registos.size() + " registos)");

                } catch (IOException e) {
                    System.out.println("recuperacao interrompida: " + e.getMessage());
                }
            }
        } finally {
            if (server != null && !server.isClosed()) {
                server.close();
            }
        }
    }
}
package rep02;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class PartitionLeader {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionLeader <k> <N> [endereço] [api]");
            return;
        }

        int k = Integer.parseInt(args[0]);
        int n = Integer.parseInt(args[1]);
        String endereco = args.length > 2 ? args[2] : "127.0.0.1";
        String api      = args.length > 3 ? args[3] : "http://127.0.0.1:5001/api/v0";

        Partitioner partitioner = new Partitioner(n);
        IpfsPubSub ps = new IpfsPubSub(api);
        RecordFile file = new RecordFile("lider-p" + k + ".txt");
        int porto = Wire.PORTO_BASE + k;

        Object lock = new Object();
        long[] ultimo = { file.lastSeq() };

        System.out.println("Líder da partição " + k + " (N=" + n + ") pronto.");
        System.out.println("Anuncia " + endereco + ":" + porto);
        System.out.println("Ficheiro: lider-p" + k + ".txt (último seq = " + ultimo[0] + ")");

        Thread entrada = new Thread(() -> {
            try {
                System.out.println("Subscrevendo " + Wire.topicIn(k));
                ps.subscribe(Wire.topicIn(k), msg -> {
                    try {
                        if (!"IN".equals(Wire.type(msg))) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        String[] f = Wire.body(msg).split(";");
                        if (f.length != 2) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        String sensor = f[0];
                        double temperatura;
                        try {
                            temperatura = Double.parseDouble(f[1]);
                        } catch (NumberFormatException e) {
                            System.out.println("REJEITADO: " + msg);
                            return;
                        }

                        
                        int destino = partitioner.of(sensor);
                        if (destino != k) {
                            System.out.println("PARTIÇÃO ERRADA: " + sensor
                                    + " pertence a p" + destino);
                            return;
                        }
                        // ▲▲▲

                        SensorRecord r;
                        synchronized (lock) {
                            long seq = ultimo[0] + 1;
                            r = SensorRecord.now(seq, sensor, temperatura);
                            file.append(r);
                            ultimo[0] = seq;
                        }

                        ps.publish(Wire.topicData(k), Wire.reg(endereco, porto, r));

                    } catch (Exception e) {
                        System.err.println("Erro ao processar IN: "
                                + e.getClass().getName() + " — " + e.getMessage());
                    }
                });
            } catch (Exception e) {
                System.err.println("Subscrição terminou: "
                        + e.getClass().getName() + " — " + e.getMessage());
            }
        });
        entrada.start();

        try (ServerSocket server = new ServerSocket(porto)) {
            System.out.println("Servidor de recuperação à escuta no porto " + porto);

            while (true) {
                try (Socket s = server.accept();
                     BufferedReader in = new BufferedReader(
                             new InputStreamReader(s.getInputStream(), StandardCharsets.UTF_8));
                     PrintWriter out = new PrintWriter(
                             new OutputStreamWriter(s.getOutputStream(), StandardCharsets.UTF_8), true)) {

                    s.setSoTimeout(Wire.T_TCP_MS);
                    String pedido = in.readLine();
                    if (pedido == null) continue;

                    long de, ate;
                    try {
                        String[] p = pedido.trim().split("\\s+");
                        if (p.length != 3 || !p[0].equals("PEDIDO")) {
                            throw new IllegalArgumentException("formato");
                        }
                        de = Long.parseLong(p[1]);
                        ate = Long.parseLong(p[2]);
                    } catch (Exception e) {
                        out.println("ERRO pedido mal formado");
                        continue;
                    }

                    if (de > ate) {
                        out.println("ERRO de > até");
                        continue;
                    }

                    long ateEfetivo;
                    synchronized (lock) {
                        ateEfetivo = Math.min(ate, ultimo[0]);
                    }

                    List<SensorRecord> regs = file.range(de, ateEfetivo);
                    for (SensorRecord r : regs) out.println(r.toLine());
                    out.println("FIM");

                    if (out.checkError()) {
                        System.out.println("Recuperação interrompida (escrita)");
                    } else {
                        System.out.println("RECUPERAÇÃO: " + de + ".." + ateEfetivo
                                + " para " + s.getRemoteSocketAddress()
                                + " (" + regs.size() + " registos)");
                    }

                } catch (IOException e) {
                    System.out.println("Recuperação interrompida: " + e.getMessage());
                }
            }
        }
    }
}
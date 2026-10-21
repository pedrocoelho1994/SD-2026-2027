package rep02;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

public class PartitionCheck {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: PartitionCheck <N> <pasta>");
            return;
        }

        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);
        Partitioner partitioner = new Partitioner(n);   

        long totalSistema = 0;

        for (int k = 0; k < n; k++) {
            System.out.println("=== Partição p" + k + " ===");

            Path liderPath = pasta.resolve("lider-p" + k + ".txt");
            List<SensorRecord> lider = ler(liderPath);

            
            boolean sensoresOk = true;
            for (SensorRecord r : lider) {
                int destino = partitioner.of(r.getSensorId());
                if (destino != k) {
                    System.out.println("  ERRO: registo seq=" + r.getSeq()
                            + " do sensor " + r.getSensorId()
                            + " devia estar em p" + destino);
                    sensoresOk = false;
                }
            }
            System.out.println("  sensores corretos: " + (sensoresOk ? "sim" : "não"));

            // 2. sequência começa em 1, sem saltos nem repetições
            boolean seqOk = true;
            long esperado = 1;
            for (SensorRecord r : lider) {
                if (r.getSeq() != esperado) {
                    System.out.println("  ERRO: esperava seq=" + esperado
                            + " encontrei seq=" + r.getSeq());
                    seqOk = false;
                    esperado = r.getSeq() + 1;
                } else {
                    esperado++;
                }
            }
            System.out.println("  sequência correta: " + (seqOk ? "sim" : "não")
                    + " (" + lider.size() + " registos)");

            // 3. réplicas que guardam esta partição
            int replicasComParticao = 0;
            try (DirectoryStream<Path> ds = Files.newDirectoryStream(pasta,
                    "replica-*-p" + k + ".txt")) {
                for (Path rPath : ds) {
                    replicasComParticao++;
                    List<SensorRecord> rep = ler(rPath);

                    boolean igual = rep.size() == lider.size();
                    if (igual) {
                        for (int i = 0; i < lider.size(); i++) {
                            if (!lider.get(i).toLine().equals(rep.get(i).toLine())) {
                                igual = false;
                                break;
                            }
                        }
                    }

                    boolean prefixo = !igual && rep.size() <= lider.size();
                    if (prefixo) {
                        for (int i = 0; i < rep.size(); i++) {
                            if (!lider.get(i).toLine().equals(rep.get(i).toLine())) {
                                prefixo = false;
                                break;
                            }
                        }
                    }

                    long faltam = lider.size() - rep.size();
                    System.out.println("  " + rPath.getFileName()
                            + ": " + rep.size() + " registos"
                            + (igual ? " (igual ao líder)"
                              : prefixo ? " (prefixo; faltam " + faltam + ")"
                              : " (DIVERGENTE)"));
                }
            }
            System.out.println("  réplicas com p" + k + ": " + replicasComParticao
                    + " (esperado R=2)");

            // 4. registos e sensores distintos
            Set<String> sensores = new HashSet<>();
            for (SensorRecord r : lider) sensores.add(r.getSensorId());
            System.out.println("  registos: " + lider.size()
                    + "  sensores distintos: " + sensores.size());

            totalSistema += lider.size();
            System.out.println();
        }

        System.out.println("Total de registos do sistema (soma das partições): " + totalSistema);
    }

    static List<SensorRecord> ler(Path p) throws IOException {
        List<SensorRecord> out = new ArrayList<>();
        if (!Files.exists(p)) return out;
        for (String line : Files.readAllLines(p, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) out.add(SensorRecord.fromLine(line));
        }
        return out;
    }
}
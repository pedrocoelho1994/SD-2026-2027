package rep03;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

public class CausalCheck {

    private record Entry(int k, SensorRecord r) { }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.out.println("Uso: CausalCheck <N> <pasta>");
            return;
        }

        int n = Integer.parseInt(args[0]);
        Path pasta = Paths.get(args[1]);

        System.out.println("=== CausalCheck (N=" + n + ", pasta=" + pasta + ") ===");

        List<Entry> allEntries = new ArrayList<>();
        boolean vkEqualsSeqGeral = true;
        int[] contagemParticao = new int[n];

        // 1. Ler ficheiros lider-p<k>.txt e verificar V[k] = seq
        for (int k = 0; k < n; k++) {
            Path file = pasta.resolve("lider-p" + k + ".txt");
            if (!Files.exists(file)) {
                System.out.println("[p" + k + "] Ficheiro nao encontrado: " + file.getFileName());
                continue;
            }

            List<String> lines = Files.readAllLines(file, StandardCharsets.UTF_8);
            boolean vkOk = true;

            for (String line : lines) {
                if (line.isBlank()) continue;
                SensorRecord r = SensorRecord.fromLine(line, n);
                allEntries.add(new Entry(k, r));
                contagemParticao[k]++;

                if (r.getVector().get(k) != r.getSeq()) {
                    System.err.println("[p" + k + "] ERRO: V[" + k + "] (" + r.getVector().get(k)
                            + ") != seq (" + r.getSeq() + ") na linha: " + line);
                    vkOk = false;
                    vkEqualsSeqGeral = false;
                }
            }

            System.out.println("[p" + k + "] " + contagemParticao[k] + " registos lidos | V[" + k + "] = seq: " + (vkOk ? "OK" : "FALHOU"));
        }

        System.out.println("\nTotal de registos no sistema: " + allEntries.size());
        System.out.println("Verificacao V[k] = seq em todas as linhas: " + (vkEqualsSeqGeral ? "VALIDADO COM SUCESSO" : "EXISTEM VIOLACOES"));

        // 2. Classificacao aos pares entre particoes diferentes
        long causais = 0;
        long concorrentes = 0;
        long concorrentesLDistinto = 0;
        long violacoesLamport = 0;

        for (int i = 0; i < allEntries.size(); i++) {
            Entry e1 = allEntries.get(i);
            for (int j = i + 1; j < allEntries.size(); j++) {
                Entry e2 = allEntries.get(j);
                if (e1.k == e2.k) {
                    continue; // apenas pares de particoes diferentes
                }

                VectorClock.Order order = e1.r.getVector().compare(e2.r.getVector());

                if (order == VectorClock.Order.BEFORE) {
                    causais++;
                    // Condicao de relogio: a -> b ==> L(a) < L(b)
                    if (e1.r.getLamport() >= e2.r.getLamport()) {
                        violacoesLamport++;
                        System.err.println("VIOLACAO DE LAMPORT: p" + e1.k + "#" + e1.r.getSeq() + " -> p" + e2.k + "#" + e2.r.getSeq()
                                + " mas L(a)=" + e1.r.getLamport() + " >= L(b)=" + e2.r.getLamport());
                    }
                } else if (order == VectorClock.Order.AFTER) {
                    causais++;
                    // Condicao de relogio: b -> a ==> L(b) < L(a)
                    if (e2.r.getLamport() >= e1.r.getLamport()) {
                        violacoesLamport++;
                        System.err.println("VIOLACAO DE LAMPORT: p" + e2.k + "#" + e2.r.getSeq() + " -> p" + e1.k + "#" + e1.r.getSeq()
                                + " mas L(b)=" + e2.r.getLamport() + " >= L(a)=" + e1.r.getLamport());
                    }
                } else {
                    // CONCURRENT ou EQUAL
                    concorrentes++;
                    if (e1.r.getLamport() != e2.r.getLamport()) {
                        concorrentesLDistinto++;
                    }
                }
            }
        }

        long totalParesCruzados = causais + concorrentes;
        System.out.println("\n=== Analise Causal entre Particoes Distintas ===");
        System.out.println("Total de pares entre particoes diferentes: " + totalParesCruzados);
        System.out.println("  - Pares causalmente relacionados (a -> b ou b -> a): " + causais);
        System.out.println("  - Pares concorrentes (a || b): " + concorrentes);

        System.out.println("\n=== Verificacao da Condicao de Relogio de Lamport ===");
        System.out.println("Violacoes da condicao de relogio (a -> b com L(a) >= L(b)): " + violacoesLamport
                + (violacoesLamport == 0 ? " (OK, satisfeita para todos os pares causais)" : " (FALHA)"));

        System.out.println("\n=== Lamport e Concorrencia ===");
        if (concorrentes > 0) {
            double prop = (concorrentesLDistinto * 100.0) / concorrentes;
            System.out.printf(Locale.ROOT, "Pares concorrentes com L distinto: %d de %d (%.2f%%)\n",
                    concorrentesLDistinto, concorrentes, prop);
        } else {
            System.out.println("Sem pares concorrentes.");
        }

        // 3. Escrever em vista-lamport.txt todos os registos ordenados por (L, k)
        allEntries.sort((e1, e2) -> {
            int cmp = Long.compare(e1.r.getLamport(), e2.r.getLamport());
            if (cmp != 0) return cmp;
            cmp = Integer.compare(e1.k, e2.k);
            if (cmp != 0) return cmp;
            return Long.compare(e1.r.getSeq(), e2.r.getSeq());
        });

        Path vistaFile = pasta.resolve("vista-lamport.txt");
        try (PrintWriter pw = new PrintWriter(Files.newBufferedWriter(vistaFile, StandardCharsets.UTF_8))) {
            for (Entry e : allEntries) {
                pw.println(e.r.toLine());
            }
        }
        System.out.println("\nVista global ordenada por (L, k) guardada em: " + vistaFile);
    }
}

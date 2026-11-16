package raft04;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.regex.*;

/** Verificador offline dos registos de eleicao e replicacao da COMINDIR05.
 * Uso: java raft04.RaftCheck <N-particoes> <pasta> [falhas.txt]
 * N e o numero de particoes; cada particao contem tres nos.
 */
public final class RaftCheck {
    private static final Pattern FILE = Pattern.compile("(eventos|no)-p(\\d+)-n(\\d+)\\.txt");
    private static final Pattern TERM = Pattern.compile("(?:^|;)termo=(\\d+)");
    private static final Pattern LEADER = Pattern.compile("(?:^|;)lider=n(\\d+)");
    private static final Pattern CANDIDATE = Pattern.compile("(?:^|;)candidato=n(\\d+)");
    private static final Pattern VOTES = Pattern.compile("votos=\\[([^]]*)\\]");
    private static int errors, warnings;

    private record Event(long time, String type, int partition, int node, String detail) {}
    private record Entry(long seq, long term, String line) {}
    private record Election(int node, long time, Set<Integer> votes) {}

    private RaftCheck() {}

    public static void main(String[] args) {
        if (args.length < 2 || args.length > 3) {
            System.err.println("Uso: RaftCheck <N-particoes> <pasta> [falhas.txt]");
            System.exit(2);
        }
        try {
            int partitions = Integer.parseInt(args[0]);
            if (partitions < 1) throw new IllegalArgumentException("N deve ser positivo");
            Path folder = Path.of(args[1]).toAbsolutePath().normalize();
            if (!Files.isDirectory(folder)) throw new IOException("Pasta inexistente: " + folder);
            System.out.println("RAFTCHECK | Pasta: " + folder + " | Particoes: " + partitions);
            for (int p = 0; p < partitions; p++) checkPartition(folder, p);
            checkProducer(folder);
            if (args.length == 3) checkFaults(Path.of(args[2]));
            System.out.printf("%nRESUMO: %d erro(s), %d aviso(s)%n", errors, warnings);
            System.out.println(errors == 0 ? "Sem violacoes detetadas nos dados analisados (nao prova formal)." : "Foram detetadas violacoes; consultar mensagens ERRO.");
            if (errors != 0) System.exit(1);
        } catch (Exception ex) {
            System.err.println("ERRO: " + ex.getMessage());
            System.exit(2);
        }
    }

    private static void checkPartition(Path folder, int p) throws IOException {
        System.out.println("\n=== PARTICAO p" + p + " ===");
        Map<Integer,List<Event>> byNode = new TreeMap<>();
        Map<Long,Map<Integer,Election>> elections = new TreeMap<>();
        Map<Long,Map<Integer,Integer>> votesByTerm = new TreeMap<>();
        int truncations = 0, recoveries = 0, candidates = 0;
        for (int node = 1; node <= 3; node++) {
            Path path = folder.resolve("eventos-p" + p + "-n" + node + ".txt");
            if (!Files.isRegularFile(path)) { warn("Falta " + path.getFileName()); continue; }
            List<Event> events = readEvents(path, p, node);
            byNode.put(node, events);
            for (Event e : events) {
                switch (e.type) {
                    case "CANDIDATO" -> candidates++;
                    case "TRUNCADO" -> truncations++;
                    case "RECUPERACAO" -> recoveries++;
                    case "LIDER" -> {
                        Long term = number(TERM, e.detail);
                        if (term == null) { warn("LIDER sem termo: " + path.getFileName()); break; }
                        Set<Integer> votes = parseVotes(e.detail);
                        if (votes.size() < 2) error("p" + p + " termo " + term + ": lider n" + node + " sem maioria documentada (votos=" + votes + ")");
                        if (!votes.contains(node)) warn("p" + p + " termo " + term + ": votos nao incluem candidato n" + node);
                        Election prev = elections.computeIfAbsent(term, t -> new TreeMap<>()).putIfAbsent(node, new Election(node,e.time,votes));
                        if (prev != null) warn("p" + p + " termo " + term + ": LIDER repetido para n" + node);
                    }
                    case "VOTO" -> {
                        Long term = number(TERM, e.detail);
                        Long candidate = number(CANDIDATE, e.detail);
                        if (term != null && candidate != null && e.detail.contains("concedido")) {
                            Integer previous = votesByTerm.computeIfAbsent(term, t -> new TreeMap<>()).putIfAbsent(node,candidate.intValue());
                            if (previous != null && previous != candidate.intValue()) error("p" + p + " n" + node + " votou em candidatos diferentes no termo " + term);
                        }
                    }
                    default -> { }
                }
            }
        }
        int electionsCount = 0;
        for (var termEntry : elections.entrySet()) {
            Map<Integer,Election> leaders = termEntry.getValue();
            electionsCount += leaders.size();
            if (leaders.size() > 1) error("p" + p + " termo " + termEntry.getKey() + ": lideres diferentes " + leaders.keySet());
            for (Election election : leaders.values()) {
                for (int voter : election.votes) {
                    if (voter < 1 || voter > 3) error("p" + p + " termo " + termEntry.getKey() + ": voto de no invalido " + voter);
                    Integer observed = votesByTerm.getOrDefault(termEntry.getKey(), Map.of()).get(voter);
                    if (observed != null && observed != election.node) error("p" + p + " termo " + termEntry.getKey() + ": voto n" + voter + " contradiz lider n" + election.node);
                }
            }
        }
        System.out.printf("Eventos: %d nos | candidaturas: %d | eleicoes confirmadas: %d | truncagens: %d | recuperacoes: %d%n",byNode.size(),candidates,electionsCount,truncations,recoveries);
        checkReplicas(folder,p);
    }

    private static List<Event> readEvents(Path file,int p,int node) throws IOException {
        List<Event> result = new ArrayList<>();
        int lineNumber = 0;
        for (String line : Files.readAllLines(file,StandardCharsets.UTF_8)) {
            lineNumber++;
            if (line.isBlank()) continue;
            String[] fields = line.split(";",4);
            if (fields.length < 4 || !fields[1].matches("[A-Z_]+") || !fields[2].equals("p"+p)) continue; // legacy descriptive lines
            try { result.add(new Event(Long.parseLong(fields[0]),fields[1],p,node,fields[3])); }
            catch (NumberFormatException ex) { warn(file.getFileName()+":"+lineNumber+" instante invalido"); }
        }
        return result;
    }

    private static void checkReplicas(Path folder,int p) throws IOException {
        Map<Integer,List<Entry>> replicas = new TreeMap<>();
        for (int n=1;n<=3;n++) {
            Path file=folder.resolve("no-p"+p+"-n"+n+".txt");
            if (!Files.isRegularFile(file)) { warn("Falta "+file.getFileName()); continue; }
            List<Entry> entries=new ArrayList<>();
            long previous=0;
            int lineNumber=0;
            for(String line:Files.readAllLines(file,StandardCharsets.UTF_8)) {
                lineNumber++;
                if(line.isBlank())continue;
                String[] f=line.split(";",-1);
                if(f.length!=7) { error(file.getFileName()+":"+lineNumber+" formato de registo invalido"); continue; }
                try {
                    long seq=Long.parseLong(f[0]),term=Long.parseLong(f[1]);
                    if(seq!=previous+1)error(file.getFileName()+": sequencia nao consecutiva em "+seq+" (anterior "+previous+")");
                    previous=seq;
                    entries.add(new Entry(seq,term,line));
                }catch(NumberFormatException ex){error(file.getFileName()+":"+lineNumber+" seq/termo invalido");}
            }
            replicas.put(n,entries);
        }
        if(replicas.isEmpty())return;
        int baseNode=replicas.keySet().iterator().next();
        List<Entry> base=replicas.get(baseNode);
        boolean equal=true;
        for(var e:replicas.entrySet()) {
            if(e.getKey()==baseNode)continue;
            if(!base.equals(e.getValue())) {
                equal=false;
                warn("p"+p+": replicas n"+baseNode+" e n"+e.getKey()+" diferem no instante da verificacao");
                int size=Math.min(base.size(),e.getValue().size());
                for(int i=0;i<size;i++)if(!base.get(i).equals(e.getValue().get(i))){
                    System.out.printf("  Primeiro conflito: seq=%d, n%d termo=%d, n%d termo=%d%n",base.get(i).seq,baseNode,base.get(i).term,e.getKey(),e.getValue().get(i).term);
                    break;
                }
            }
        }
        System.out.printf("Replicas: %d/3 encontradas | registos n%d: %d | conteudo: %s%n",replicas.size(),baseNode,base.size(),equal&&replicas.size()==3?"IDENTICO":"INCOMPLETO/DIFERENTE");
    }

    private static void checkProducer(Path folder) throws IOException {
        Path file=folder.resolve("produtor.txt");
        if(!Files.isRegularFile(file)) { warn("produtor.txt ausente: nao e possivel calcular leituras perdidas"); return; }
        Map<Integer,Map<String,Integer>> published = new TreeMap<>();
        int malformed=0, total=0;
        for (String line:Files.readAllLines(file,StandardCharsets.UTF_8)) {
            if (line.isBlank()) continue;
            String[] fields=line.split(";",-1);
            if (fields.length!=4 || !fields[1].matches("p\\d+")) { malformed++; continue; }
            try {
                Long.parseLong(fields[0]);
                int partition=Integer.parseInt(fields[1].substring(1));
                String key=fields[2]+";"+Double.toString(Double.parseDouble(fields[3]));
                published.computeIfAbsent(partition,k->new TreeMap<>()).merge(key,1,Integer::sum);
                total++;
            } catch(NumberFormatException ex) { malformed++; }
        }
        System.out.println("\nProdutor: "+total+" publicacoes registadas; linhas invalidas: "+malformed);
        if(malformed>0)warn("produtor.txt contem linhas fora do formato instante;pN;sensor;temperatura");
        for(var part:published.entrySet()) {
            int partition=part.getKey();
            Path replica=folder.resolve("no-p"+partition+"-n1.txt");
            if(!Files.isRegularFile(replica)) { warn("p"+partition+": falta replica n1 para comparar leituras");continue; }
            Map<String,Integer> stored=new HashMap<>();
            for(String line:Files.readAllLines(replica,StandardCharsets.UTF_8)) {
                String[] f=line.split(";",-1);
                if(f.length!=7)continue;
                try {
                    String key=f[2]+";"+Double.toString(Double.parseDouble(f[3]));
                    stored.merge(key,1,Integer::sum);
                }catch(NumberFormatException ignored) { }
            }
            int matched=0, missing=0;
            for(var item:part.getValue().entrySet()) {
                int found=Math.min(item.getValue(),stored.getOrDefault(item.getKey(),0));
                matched+=found;
                missing+=item.getValue()-found;
            }
            System.out.printf("p%d: publicadas=%d | correspondencias por sensor/temperatura=%d | sem correspondencia=%d%n",
                    partition,part.getValue().values().stream().mapToInt(Integer::intValue).sum(),matched,missing);
            if(missing>0)warn("p"+partition+": "+missing+" publicacoes sem correspondencia na replica n1 (pode haver atraso ou perda)");
        }
        System.out.println("NOTA: comparacao aproximada; leituras repetidas, execucoes antigas e alarmes impedem prova de perda individual.");
    }
    private static void checkFaults(Path file) throws IOException {
        if(!Files.isRegularFile(file)) { warn("Ficheiro de falhas nao encontrado: "+file);return; }
        long lines=Files.lines(file,StandardCharsets.UTF_8).filter(s->!s.isBlank()).count();
        System.out.println("Falhas: "+lines+" linhas encontradas; indisponibilidade nao calculada sem esquema temporal confirmado.");
    }
    private static Long number(Pattern pattern,String text) {
        Matcher m=pattern.matcher(text);
        return m.find()?Long.parseLong(m.group(1)):null;
    }
    private static Set<Integer> parseVotes(String text) {
        Matcher m=VOTES.matcher(text);
        Set<Integer> votes=new TreeSet<>();
        if(!m.find())return votes;
        for(String s:m.group(1).split(",")) {
            if(s.isBlank())continue;
            try { votes.add(Integer.parseInt(s.trim())); }
            catch(NumberFormatException ex){warn("Lista de votos invalida: "+text);}
        }
        return votes;
    }
    private static void warn(String message){warnings++;System.out.println("AVISO: "+message);}
    private static void error(String message){errors++;System.out.println("ERRO: "+message);}
}

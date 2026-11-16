
package raft04;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public final class PartitionNode {

    private final int partition;
    private final int nodeId;
    private volatile boolean running = true;

    private final RecordFile records;
    private final RaftState raft;
    private final IpfsPubSub pubsub;
    private final Partitioner partitioner;

    private final Object dataLock = new Object();
    private final List<SensorRecord> pending = new ArrayList<>();

    private final java.util.concurrent.atomic.AtomicBoolean
        recoveryInProgress =
        new java.util.concurrent.atomic.AtomicBoolean(false);

    // Último prefixo confirmado por TCP contra o líder atual.
    private long verifiedTerm = -1;
    private int verifiedLeader = -1;
    private long verifiedSeq = -1;

    private long heartbeatNumber = 0;
    private long currentLamport;
    private VectorClock currentVector;

    public PartitionNode(int partition, int nodeId) throws IOException {

        if (partition < 0 || partition >= 3) {
            throw new IllegalArgumentException(
                    "Partição inválida: " + partition);
        }

        if (nodeId < 1 || nodeId > Wire.GRUPO) {
            throw new IllegalArgumentException(
                    "Identificador de nó inválido: " + nodeId);
        }

        this.partition = partition;
        this.nodeId = nodeId;

        this.pubsub = new IpfsPubSub(
                "http://127.0.0.1:5001/api/v0");

        this.partitioner = new Partitioner(3);

        String suffix = "p" + partition + "-n" + nodeId;

        this.records = new RecordFile("no-" + suffix + ".txt");

        this.raft = new RaftState(
                partition,
                nodeId,
                Wire.GRUPO,
                Wire.ELEICAO_MIN_MS,
                Wire.ELEICAO_MAX_MS,
                Path.of("estado-" + suffix + ".txt"),
                this::logEvent);

        SensorRecord last = records.last(3);

        this.currentLamport =
                last == null ? 0 : last.lamport;

        this.currentVector =
                last == null
                        ? new VectorClock(3)
                        : last.vector.copy();
    }

  private synchronized void logEvent(String event) {
    long instante = System.currentTimeMillis();

    String linha = instante + ";" + event;

    System.out.println(
            "[p" + partition + "-n" + nodeId + "] " + event);

    try {
        java.nio.file.Files.writeString(
                java.nio.file.Path.of(
                        "eventos-p" + partition + "-n" + nodeId + ".txt"),
                linha + System.lineSeparator(),
                java.nio.charset.StandardCharsets.UTF_8,
                java.nio.file.StandardOpenOption.CREATE,
                java.nio.file.StandardOpenOption.APPEND
        );
    } catch (java.io.IOException e) {
        System.err.println(
                "Erro ao gravar evento: " + e.getMessage());
    }
}

/**
 * Servidor TCP para recuperação de registos.
 * Todos os nós podem fornecer os seus registos locais,
 * exceto quando estão isolados.
 */
    private void tcpServerLoop() {

        int port = Wire.nodePort(partition, nodeId);

        try (ServerSocket server = new ServerSocket()) {

            server.bind(new InetSocketAddress("127.0.0.1", port));

            logEvent("Servidor TCP iniciado na porta " + port);

            while (running) {

                try {
                    Socket client = server.accept();

                    new Thread(
                            () -> handleTcpClient(client),
                            "tcp-cliente-p" + partition + "-n" + nodeId
                    ).start();

                } catch (IOException e) {
                    if (running) {
                        logEvent("ERRO TCP: " + e.getMessage());
                    }
                }
            }

        } catch (IOException e) {
            logEvent(
                    "ERRO ao iniciar servidor TCP: " + e.getMessage());
        }
    }

    /**
     * Processa pedidos de recuperação recebidos por TCP.
     * Protocolo: PEDIDO <de> <ate>
     */
    private void handleTcpClient(Socket client) {

        try (Socket socket = client;
            BufferedReader input = new BufferedReader(
                    new InputStreamReader(
                            socket.getInputStream(),
                            StandardCharsets.UTF_8));
            PrintWriter output = new PrintWriter(
                    new OutputStreamWriter(
                            socket.getOutputStream(),
                            StandardCharsets.UTF_8), true)) {

            socket.setSoTimeout(Wire.T_TCP_MS);

            String request = input.readLine();

            if (request == null) {
                return;
            }

            String[] fields = request.trim().split("\\s+");

            if (fields.length != 3
                    || !"PEDIDO".equals(fields[0])) {

                output.println("ERRO pedido invalido");
                return;
            }

            long from = Long.parseLong(fields[1]);
            long to = Long.parseLong(fields[2]);

            if (from < 1 || to < from) {
                output.println("ERRO intervalo invalido");
                return;
            }

            synchronized (dataLock) {

                if (Wire.isolated(partition, nodeId)) {

                    output.println("ERRO no isolado");
                    return;
                }

                long lastSeq = records.lastSeq();

                if (to > lastSeq) {
                    output.println("ERRO intervalo indisponivel");
                    return;
                }

                List<SensorRecord> missing =
                        records.range(from, to);

                long expected = from;

                for (SensorRecord record : missing) {

                    if (record.getSeq() != expected) {
                        output.println("ERRO lacuna no lider");
                        return;
                    }

                    expected++;
                }

                if (expected != to + 1) {
                    output.println("ERRO registos incompletos");
                    return;
                }

                for (SensorRecord record : missing) {
                    output.println(record.toLine());
                }

                output.println("FIM");

                logEvent(
                        "RECUPERAÇÃO TCP enviada: seq="
                                + from + ".." + to);
            }

        } catch (Exception e) {

            logEvent(
                    "ERRO ao processar pedido TCP: "
                            + e.getMessage());
        }
    }


/**
 * Recupera, por TCP, os registos em falta no nó seguidor.
 * A resposta é validada antes de atualizar o ficheiro local.
 */
    /**
     * Reconcilia o prefixo completo com o líder, incluindo divergências
     * com o mesmo seq e divergências em que o líder tem menos registos.
     * A substituição é atómica no RecordFile.
     */
    private void recoverMissingRecords(
            String address, int port, long term, int leader, long targetSeq) {
        try {
            if (Wire.isolated(partition, nodeId)) return;

            List<SensorRecord> received = new ArrayList<>();
            if (targetSeq > 0) {
                try (Socket socket = new Socket()) {
                    socket.connect(new InetSocketAddress(address, port), Wire.T_TCP_MS);
                    socket.setSoTimeout(Wire.T_TCP_MS);
                    try (BufferedReader input = new BufferedReader(
                            new InputStreamReader(socket.getInputStream(), StandardCharsets.UTF_8));
                         PrintWriter output = new PrintWriter(
                            new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8), true)) {
                        output.println("PEDIDO 1 " + targetSeq);
                        long expected = 1;
                        boolean finished = false;
                        String line;
                        while ((line = input.readLine()) != null) {
                            if ("FIM".equals(line)) {
                                finished = true;
                                break;
                            }
                            if (line.startsWith("ERRO")) {
                                throw new IOException("Pedido TCP recusado: " + line);
                            }
                            SensorRecord record = SensorRecord.fromLine(line, Wire.GRUPO);
                            if (record.getSeq() != expected++
                                    || record.getTerm() < 1 || record.getTerm() > term
                                    || record.getSeq() > targetSeq) {
                                throw new IOException("Registo TCP inválido: " + line);
                            }
                            received.add(record);
                        }
                        if (!finished || expected != targetSeq + 1) {
                            throw new IOException("Resposta TCP incompleta");
                        }
                    }
                }
            }

            synchronized (dataLock) {
                if (Wire.isolated(partition, nodeId)
                        || raft.term() != term || raft.termIfLeader() >= 0) {
                    logEvent("IGNORADO;p" + partition + ";recuperação cancelada");
                    return;
                }

                List<SensorRecord> local = records.all();
                int common = 0;
                while (common < local.size() && common < received.size()
                        && local.get(common).toLine().equals(received.get(common).toLine())) {
                    common++;
                }
                if (common < local.size()) {
                    logEvent("TRUNCADO;p" + partition + ";desdeSeq=" + (common + 1)
                            + ";removidos=" + (local.size() - common));
                }
                if (common != local.size() || common != received.size()) {
                    records.replaceAll(received);
                    logEvent("RECUPERACAO;p" + partition + ";desdeSeq=" + (common + 1)
                            + ";ultimoSeq=" + targetSeq + ";lider=n" + leader);
                }
                SensorRecord last = received.isEmpty() ? null : received.get(received.size() - 1);
                currentLamport = last == null ? 0 : last.lamport;
                currentVector = last == null ? new VectorClock(3) : last.vector.copy();
                verifiedTerm = term;
                verifiedLeader = leader;
                verifiedSeq = targetSeq;
            }
        } catch (Exception e) {
            logEvent("ERRO;p" + partition + ";recuperação TCP: " + e.getMessage());
        } finally {
            recoveryInProgress.set(false);
        }
    }


    private void publishVoteRequest(long electionTerm)
            throws IOException, InterruptedException {

        VoteRequest request = new VoteRequest(
                electionTerm,
                nodeId,
                records.lastSeq(),
                records.lastTerm());

        if (Wire.isolated(partition, nodeId)) {
            return;
        }

        pubsub.publish(
                Wire.topicElection(partition),
                request.encode());

        logEvent("PEDIDO_VOTO enviado no termo " + electionTerm);
    }

    private void publishHeartbeat()
            throws IOException, InterruptedException {

        if (Wire.isolated(partition, nodeId)) {
            return;
        }

        synchronized (dataLock) {

            long leaderTerm = raft.termIfLeader();

            if (leaderTerm < 0) {
                return;
            }

            long lastSeq = records.lastSeq();
            long lastTerm = records.lastTerm();
            long hb = ++heartbeatNumber;

            List<SensorRecord> batchRecords =
                    new ArrayList<>(pending);

            Batch batch = new Batch(
                    leaderTerm,
                    nodeId,
                    "127.0.0.1",
                    Wire.nodePort(partition, nodeId),
                    hb,
                    lastSeq,
                    lastTerm,
                    batchRecords);

            pubsub.publish(
                    Wire.topicData(partition),
                    batch.encode());

            records.appendHeartbeat(
                    leaderTerm,
                    hb,
                    System.currentTimeMillis(),
                    lastSeq,
                    lastTerm,
                    batchRecords.size(),
                    batch.byteLength());

            pending.clear();

            logEvent(
                    "LOTE enviado: termo=" + leaderTerm
                            + ", hb=" + hb
                            + ", registos=" + batchRecords.size()
                            + ", ultimoSeq=" + lastSeq);
        }
    }

    private void handleSensorMessage(String message) {

        try {

            String[] fields = message.trim().split(";", -1);

            if (fields.length != 5
                    || !"IN".equals(fields[0])
                    || fields[1].isBlank()) {

                throw new IllegalArgumentException(
                        "Mensagem IN mal formada");
            }

            String sensorId = fields[1];

            if (partitioner.of(sensorId) != partition) {
                return;
            }

            double temperature =
                    Double.parseDouble(fields[2]);

            if (!Double.isFinite(temperature)) {
                throw new IllegalArgumentException(
                        "Temperatura inválida");
            }

            long receivedLamport =
                    Long.parseLong(fields[3]);

            VectorClock receivedVector =
                    VectorClock.parse(fields[4], 3);

            synchronized (dataLock) {

                long leaderTerm = raft.termIfLeader();

                if (leaderTerm < 0) {
                    return;
                }

                long nextSeq = records.lastSeq() + 1;

                long nextLamport =
                        Math.max(currentLamport, receivedLamport) + 1;

                VectorClock nextVector =
                        currentVector.copy();

                nextVector.merge(receivedVector);
                nextVector.tick(partition);

                SensorRecord record = SensorRecord.now(
                        nextSeq,
                        leaderTerm,
                        sensorId,
                        temperature,
                        nextLamport,
                        nextVector.copy());

                records.append(record);

                currentLamport = nextLamport;
                currentVector = nextVector;

                pending.add(record);

                logEvent(
                        "EVENTO recebido: seq=" + nextSeq
                                + ", termo=" + leaderTerm
                                + ", sensor=" + sensorId
                                + ", temperatura=" + temperature
                                + ", L=" + nextLamport
                                + ", V=" + nextVector);
            }

        } catch (Exception e) {

            logEvent(
                    "ERRO ao processar IN: " + e.getMessage());
        }
    }

    private void handleElectionMessage(String message) {

        try {

            if (Wire.isolated(partition, nodeId)) {
                return;
            }

            String type = Wire.type(message);

            if ("PEDIDO_VOTO".equals(type)) {
                handleVoteRequest(message);
            } else if ("VOTO".equals(type)) {
                handleVoteReply(message);
            }

        } catch (Exception e) {

            logEvent(
                    "ERRO ao tratar mensagem de eleição: "
                            + e.getMessage());
        }
    }

    private void handleVoteRequest(String message)
            throws IOException, InterruptedException {

        VoteRequest request = VoteRequest.decode(message);

        if (request.candidate == nodeId) {
            return;
        }

        RaftState.Vote vote = raft.onVoteRequest(
                request.term,
                request.candidate,
                request.lastSeq,
                request.lastTerm,
                records.lastSeq(),
                records.lastTerm());

        VoteReply reply = new VoteReply(
                vote.term,
                nodeId,
                request.candidate,
                vote.granted);

        if (Wire.isolated(partition, nodeId)) {
            return;
        }

        pubsub.publish(
                Wire.topicElection(partition),
                reply.encode());

        logEvent(
                "Resposta ao pedido de voto de n"
                        + request.candidate
                        + ": "
                        + (vote.granted ? "SIM" : "NÃO"));
    }

    private void handleVoteReply(String message)
            throws IOException, InterruptedException {

        VoteReply reply = VoteReply.decode(message);

        if (reply.candidate != nodeId
                || reply.voter == nodeId) {
            return;
        }

        boolean becameLeader = raft.onVote(
                reply.term,
                reply.voter,
                reply.candidate,
                reply.granted);

        if (becameLeader) {

            logEvent(
                    "ELEITO LÍDER no termo " + reply.term);

            synchronized (dataLock) {
                pending.clear();
                heartbeatNumber = 0;
            }

            publishHeartbeat();
        }
    }

    private void handleDataMessage(String message) {

        try {

            if (Wire.isolated(partition, nodeId)) {
                return;
            }

            Batch batch = Batch.decode(
                    message,
                    Wire.GRUPO);

            if (batch.leader == nodeId) {
                return;
            }

            synchronized (dataLock) {

                boolean accepted = raft.onBatch(
                        batch.term,
                        batch.leader);

                if (!accepted) {
                    return;
                }

                // Não acrescentar registos antes de verificar o prefixo:
                // o mesmo seq pode conter dados de termos diferentes.
                if ((verifiedTerm != batch.term || verifiedLeader != batch.leader
                        || verifiedSeq != batch.lastSeq
                        || records.lastSeq() != batch.lastSeq
                        || records.lastTerm() != batch.lastTerm)
                        && recoveryInProgress.compareAndSet(false, true)) {
                    new Thread(() -> recoverMissingRecords(
                            batch.address, batch.port, batch.term,
                            batch.leader, batch.lastSeq),
                            "tcp-recuperacao-p" + partition + "-n" + nodeId).start();
                }

                records.appendHeartbeat(
                        batch.term,
                        batch.hb,
                        System.currentTimeMillis(),
                        batch.lastSeq,
                        batch.lastTerm,
                        batch.records.size(),
                        batch.byteLength());

                logEvent(
                        "HEARTBEAT recebido de n"
                                + batch.leader
                                + " no termo "
                                + batch.term);
            }

        } catch (Exception e) {

            logEvent(
                    "ERRO ao tratar LOTE: " + e.getMessage());
        }
    }

    private void electionSubscriptionLoop() {

        while (running) {

            try {

                pubsub.subscribe(
                        Wire.topicElection(partition),
                        this::handleElectionMessage);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;

            } catch (IOException e) {

                logEvent(
                        "ERRO na subscrição de eleição: "
                                + e.getMessage());

                sleepBeforeRetry();
            }
        }
    }

    private void dataSubscriptionLoop() {

        while (running) {

            try {

                pubsub.subscribe(
                        Wire.topicData(partition),
                        this::handleDataMessage);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;

            } catch (IOException e) {

                logEvent(
                        "ERRO na subscrição de dados: "
                                + e.getMessage());

                sleepBeforeRetry();
            }
        }
    }

    private void sensorSubscriptionLoop() {

        while (running) {

            try {

                pubsub.subscribe(
                        Wire.topicIn(partition),
                        this::handleSensorMessage);

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;

            } catch (IOException e) {

                logEvent(
                        "ERRO na subscrição de sensores: "
                                + e.getMessage());

                sleepBeforeRetry();
            }
        }
    }

    private void sleepBeforeRetry() {

        try {

            Thread.sleep(1000);

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();
            running = false;
        }
    }

    private void electionLoop() {

        while (running) {

            try {

                Thread.sleep(Wire.VERIFICACAO_MS);

                // Mesmo isolado, o nó continua a atualizar os seus
                // temporizadores e o termo persistido.
                long electionTerm = raft.tick();

                // O isolamento impede apenas a publicação de eleições.
                if (Wire.isolated(partition, nodeId)) {
                    continue;
                }

                if (electionTerm > 0) {

                    logEvent(
                            "Iniciada eleição no termo "
                                    + electionTerm);

                    publishVoteRequest(electionTerm);
                }

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;

            } catch (IOException e) {

                logEvent(
                        "ERRO na eleição: "
                                + e.getMessage());
            }
        }
    }

    private void heartbeatLoop() {

        while (running) {

            try {

                Thread.sleep(Wire.PERIODO_HB_MS);

                publishHeartbeat();

            } catch (InterruptedException e) {

                Thread.currentThread().interrupt();
                break;

            } catch (IOException e) {

                logEvent(
                        "ERRO ao enviar heartbeat: "
                                + e.getMessage());
            }
        }
    }

    public void start() {

        logEvent(
                "Nó iniciado como "
                        + raft.role()
                        + " no termo "
                        + raft.term());

        new Thread(
                this::tcpServerLoop,
                "tcp-servidor-p" + partition + "-n" + nodeId
        ).start();

        new Thread(
                this::electionSubscriptionLoop,
                "eleicao-sub-p" + partition + "-n" + nodeId
        ).start();

        new Thread(
                this::dataSubscriptionLoop,
                "dados-sub-p" + partition + "-n" + nodeId
        ).start();

        new Thread(
                this::sensorSubscriptionLoop,
                "sensores-sub-p" + partition + "-n" + nodeId
        ).start();

        new Thread(
                this::electionLoop,
                "eleicao-timer-p" + partition + "-n" + nodeId
        ).start();

        new Thread(
                this::heartbeatLoop,
                "heartbeat-p" + partition + "-n" + nodeId
        ).start();
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {

            System.err.println(
                    "Utilização: java raft04.PartitionNode "
                            + "<particao> <no>");

            return;
        }

        int partition = Integer.parseInt(args[0]);
        int nodeId = Integer.parseInt(args[1]);

        new PartitionNode(partition, nodeId).start();
    }
}

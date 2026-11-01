import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IpfsDaemon {
    private static final String EXECUTABLE = System.getProperty("ipfs.executable", "ipfs");
    private static final int API_NODE_1 = 5001;
    private static final int API_NODE_2 = 5002;
    private static final int SWARM_NODE_1 = 4001;
    private static final int SWARM_NODE_2 = 4002;
    private static final Path REPO_NODE_2 = Path.of(
            System.getProperty("user.home"), ".ipfs-sd-comindir02-node2");
    private static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(2))
            .build();

    public static void main(String[] args) throws Exception {
        startDefaultNode();
        startSecondNode();
        connectNodes();
        System.out.println("Dois nós Kubo prontos: APIs em 127.0.0.1:"
                + API_NODE_1 + " e 127.0.0.1:" + API_NODE_2 + ".");
    }

    private static void startDefaultNode() throws Exception {
        if (apiIsUp(API_NODE_1)) {
            System.out.println("A reutilizar o nó Kubo existente na API " + API_NODE_1 + ".");
            return;
        }

        Process node = new ProcessBuilder(EXECUTABLE, "daemon", "--init")
                .inheritIO()
                .start();
        waitForApi(API_NODE_1, node, "primeiro nó");
    }

    private static void startSecondNode() throws Exception {
        if (apiIsUp(API_NODE_2)) {
            System.out.println("A reutilizar o nó Kubo existente na API " + API_NODE_2 + ".");
            return;
        }

        Files.createDirectories(REPO_NODE_2);
        if (!Files.exists(REPO_NODE_2.resolve("config"))) {
            runIpfs(REPO_NODE_2, "init", "--empty-repo=true");
        }
        runIpfs(REPO_NODE_2, "bootstrap", "rm", "--all");
        runIpfs(REPO_NODE_2, "config", "Addresses.API", "/ip4/127.0.0.1/tcp/" + API_NODE_2);
        runIpfs(REPO_NODE_2, "config", "Addresses.Gateway", "/ip4/127.0.0.1/tcp/8081");
        runIpfs(REPO_NODE_2, "config", "--bool", "Pubsub.Enabled", "true");
        runIpfs(REPO_NODE_2, "config", "--json", "Addresses.Swarm",
                "[\\\"/ip4/127.0.0.1/tcp/" + SWARM_NODE_2
                        + "\\\",\\\"/ip4/127.0.0.1/udp/" + SWARM_NODE_2 + "/quic-v1\\\"]");

        ProcessBuilder builder = new ProcessBuilder(EXECUTABLE, "daemon").inheritIO();
        builder.environment().put("IPFS_PATH", REPO_NODE_2.toString());
        Process daemon = builder.start();
        waitForApi(API_NODE_2, daemon, "segundo nó");
    }

    private static void connectNodes() throws Exception {
        String peerId = getPeerId(API_NODE_1);
        String address = "/ip4/127.0.0.1/tcp/" + SWARM_NODE_1 + "/p2p/" + peerId;
        URI uri = URI.create("http://127.0.0.1:" + API_NODE_2
                + "/api/v0/swarm/connect?arg="
                + URLEncoder.encode(address, StandardCharsets.UTF_8));
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("Nao foi possivel ligar o segundo no ao primeiro: HTTP "
                    + response.statusCode() + " " + response.body());
        }
        System.out.println("Ligacao entre os nos IPFS estabelecida.");
    }

    private static String getPeerId(int apiPort) throws Exception {
        URI uri = URI.create("http://127.0.0.1:" + apiPort + "/api/v0/id");
        HttpRequest request = HttpRequest.newBuilder(uri)
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 200) {
            throw new IOException("A API Kubo " + apiPort + " respondeu HTTP " + response.statusCode());
        }
        Matcher matcher = Pattern.compile("\\\"ID\\\"\\s*:\\s*\\\"([^\\\"]+)\\\"").matcher(response.body());
        if (!matcher.find()) {
            throw new IOException("A resposta da API Kubo " + apiPort + " nao contem o ID do no.");
        }
        return matcher.group(1);
    }

    private static boolean apiIsUp(int port) throws InterruptedException {
        try {
            URI uri = URI.create("http://127.0.0.1:" + port + "/api/v0/id");
            HttpRequest request = HttpRequest.newBuilder(uri)
                    .timeout(Duration.ofSeconds(2))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .build();
            return HTTP.send(request, HttpResponse.BodyHandlers.discarding()).statusCode() == 200;
        } catch (IOException e) {
            return false;
        }
    }

    private static void waitForApi(int port, Process process, String description) throws Exception {
        for (int i = 0; i < 30; i++) {
            if (apiIsUp(port)) {
                return;
            }
            if (!process.isAlive()) {
                throw new IOException("O processo do " + description + " terminou antes da API "
                        + port + " ficar pronta (codigo " + process.exitValue() + ").");
            }
            Thread.sleep(1000);
        }
        throw new IOException("O " + description + " nao respondeu na API " + port + " em 30 segundos.");
    }

    private static void runIpfs(Path repo, String... arguments) throws Exception {
        List<String> command = new ArrayList<>();
        command.add(EXECUTABLE);
        command.addAll(List.of(arguments));
        ProcessBuilder builder = new ProcessBuilder(command).redirectErrorStream(true);
        builder.environment().put("IPFS_PATH", repo.toString());
        Process process = builder.start();
        String output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new IOException("Comando Kubo falhou (" + String.join(" ", command)
                    + "): " + output.trim());
        }
        if (!output.isBlank()) {
            System.out.print(output);
        }
    }
}
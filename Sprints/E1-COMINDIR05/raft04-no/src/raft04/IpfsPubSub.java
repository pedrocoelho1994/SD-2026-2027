
package raft04;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.function.Consumer;
import java.util.stream.Stream;

/**
 * Adaptador HTTP para publicação e subscrição PubSub do Kubo.
 *
 * Trata interrupções das ligações HTTP para permitir que
 * o PartitionNode volte a estabelecer as subscrições.
 */
public class IpfsPubSub {

    private final String api;

    private static final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    public IpfsPubSub(String api) {
        this.api = api;
    }

    /** Codifica texto em multibase base64url. */
    static String toMultibase(String s) {
        return "u" + Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(s.getBytes(StandardCharsets.UTF_8));
    }

    /** Descodifica texto multibase base64url. */
    static String fromMultibase(String s) {
        if (!s.startsWith("u")) {
            throw new IllegalArgumentException(
                    "Codificacao multibase nao suportada: " + s);
        }

        return new String(
                Base64.getUrlDecoder().decode(s.substring(1)),
                StandardCharsets.UTF_8);
    }

    /** Publica uma mensagem no tópico indicado. */
    public void publish(String topic, String message)
            throws IOException, InterruptedException {

        String boundary = "raft04-" + System.nanoTime();

        String body = "--" + boundary + "\r\n"
                + "Content-Disposition: form-data; name=\"file\"; filename=\"msg\"\r\n"
                + "Content-Type: application/octet-stream\r\n\r\n"
                + message + "\r\n"
                + "--" + boundary + "--\r\n";

        HttpRequest req = HttpRequest.newBuilder(
                        URI.create(api + "/pubsub/pub?arg=" + toMultibase(topic)))
                .header("Content-Type",
                        "multipart/form-data; boundary=" + boundary)
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(
                        body, StandardCharsets.UTF_8))
                .build();

        HttpResponse<String> resp =
                http.send(req, HttpResponse.BodyHandlers.ofString());

        if (resp.statusCode() != 200) {
            throw new IOException(
                    "Publicacao falhou: topico=" + topic
                    + ", estado HTTP=" + resp.statusCode()
                    + ", resposta=" + resp.body());
        }
    }

    /**
     * Mantém uma subscrição bloqueante.
     *
     * Se a ligação terminar ou for interrompida,
     * devolve uma IOException ao PartitionNode,
     * permitindo que este tente subscrever novamente.
     */
    public void subscribe(String topic, Consumer<String> onMessage)
            throws IOException, InterruptedException {

        HttpRequest req = HttpRequest.newBuilder(
                        URI.create(api + "/pubsub/sub?arg=" + toMultibase(topic)))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Stream<String>> resp =
                http.send(req, HttpResponse.BodyHandlers.ofLines());

        if (resp.statusCode() != 200) {
            try (Stream<String> body = resp.body()) {
                throw new IOException(
                        "Subscricao falhou: topico=" + topic
                        + ", estado HTTP=" + resp.statusCode());
            }
        }

        try (Stream<String> lines = resp.body()) {

            lines.filter(json -> json.contains("\"data\""))
                    .forEach(json -> {
                        try {
                            String data = extractField(json, "data");
                            onMessage.accept(fromMultibase(data));
                        } catch (IllegalArgumentException e) {
                            System.err.println(
                                    "Mensagem PubSub invalida no topico "
                                    + topic + ": " + e.getMessage());
                        }
                    });

            // A subscrição terminou inesperadamente.
            throw new IOException(
                    "Ligacao PubSub terminada no topico " + topic);

        } catch (UncheckedIOException e) {
            throw new IOException(
                    "Ligacao PubSub interrompida no topico " + topic,
                    e.getCause());
        }
    }

    /** Extrai um campo textual do JSON recebido do Kubo. */
    static String extractField(String json, String field) {

        String key = "\"" + field + "\":\"";
        int start = json.indexOf(key);

        if (start < 0) {
            throw new IllegalArgumentException(
                    "Campo '" + field + "' ausente: " + json);
        }

        start += key.length();

        int end = json.indexOf('"', start);

        if (end < 0) {
            throw new IllegalArgumentException(
                    "Campo '" + field + "' incompleto: " + json);
        }

        return json.substring(start, end);
    }
}

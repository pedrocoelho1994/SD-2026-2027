package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.entities.emoji.Emoji;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionMapping;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class LeaderBot extends ListenerAdapter {

    // Comandos que exigem o papel admin-arquivo
    private static final Set<String> ADMIN_COMMANDS = Set.of(
            "config", "replica-token", "replica-remover", "rajada"
    );

    private static final String ADMIN_ROLE = "admin-arquivo";
    private static final String ARQUIVO_CHANNEL = "arquivo";

    private final Path dataDir;
    private final Path seladosDir;
    private final byte[] masterKey;
    private final Path eventosPath;
    private final FileChannel registosChannel;
    private final ScheduledExecutorService scheduler;
    private long ultimoLsn;
    private Chunk chunkAtual;
    private ScheduledFuture<?> selagemFuture;

    public LeaderBot(Path dataDir, byte[] masterKey) throws IOException {
        this.dataDir = dataDir;
        this.seladosDir = dataDir.resolve("selados");
        this.masterKey = masterKey;
        Files.createDirectories(dataDir);
        Files.createDirectories(seladosDir);

        Path registosPath = dataDir.resolve("registos.log");
        this.eventosPath = dataDir.resolve("eventos-lider.txt");
        this.ultimoLsn = carregarUltimoLsn(registosPath);
        this.registosChannel = FileChannel.open(
                registosPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        );
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "chunk-selagem-timer");
            t.setDaemon(true);
            return t;
        });
    }

    public static void main(String[] args) throws InterruptedException {
        // 1. Validar parâmetros Wire e carregar/gerar master.key + estado local antes de ligar o Discord
        Path dataDir = Path.of("dados-lider");
        LeaderBot bot;
        try {
            int maxBytes = Wire.CHUNK_MAX_BYTES;
            byte[] masterKey = ChunkCrypto.loadOrCreateMasterKey(dataDir.resolve("master.key"));
            bot = new LeaderBot(dataDir, masterKey);
            System.out.println("Estado local pronto (CHUNK_MAX_BYTES=" + maxBytes
                    + ", ultimoLsn=" + bot.ultimoLsn + ")");
        } catch (Exception e) {
            System.err.println("ERRO FATAL no arranque local: " + e.getMessage());
            return;
        }

        // 2. Ler as variáveis de ambiente estritamente do launch.json
        String token = System.getenv("DISCORD_TOKEN");
        String guildId = System.getenv("DISCORD_GUILD_ID");

        // Verificação de segurança (se falhar, nao arranca)
        if (token == null || token.isBlank()) {
            System.err.println("ERRO FATAL: Variavel DISCORD_TOKEN nao definida no launch.json.");
            return;
        }
        if (guildId == null || guildId.isBlank()) {
            System.err.println("ERRO FATAL: Variavel DISCORD_GUILD_ID nao definida no launch.json.");
            return;
        }

        // 3. Construir o JDA com os Intents necessários
        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT, GatewayIntent.GUILD_MESSAGES)
                .addEventListeners(bot)
                .build();

        jda.awaitReady();
        System.out.println("Bot ligado como " + jda.getSelfUser().getName());

        // 4. Registar os comandos no servidor de teste (Guild Commands)
        Guild guild = jda.getGuildById(guildId);
        if (guild == null) {
            System.err.println("ERRO: servidor nao encontrado. Verifica o Guild ID.");
            return;
        }

        guild.updateCommands().addCommands(
                Commands.slash("registar", "Registar um texto")
                        .addOption(OptionType.STRING, "texto", "Texto a registar", true),
                Commands.slash("estado", "Ver estado do sistema"),
                Commands.slash("config", "Configurar parâmetro r")
                        .addOption(OptionType.INTEGER, "r", "Valor de r", true),
                Commands.slash("recuperar", "Recuperar intervalo de LSN")
                        .addOption(OptionType.INTEGER, "de", "LSN inicial", true)
                        .addOption(OptionType.INTEGER, "ate", "LSN final", true),
                Commands.slash("replica-token", "Gerar token para nova réplica"),
                Commands.slash("replica-remover", "Remover réplica")
                        .addOption(OptionType.STRING, "id", "ID da réplica", true),
                Commands.slash("rajada", "Injetar registos sintéticos")
                        .addOption(OptionType.INTEGER, "n", "Número de registos", true)
                        .addOption(OptionType.INTEGER, "intervalo", "Intervalo entre registos (ms)", true)
        ).queue(
                success -> System.out.println("✅ Comandos registados com sucesso no servidor " + guild.getName()),
                error -> System.err.println("❌ ERRO CRITICO ao registar comandos: " + error.getMessage())
        );
    }

    private static long carregarUltimoLsn(Path logPath) throws IOException {
        if (Files.notExists(logPath)) {
            return 0L;
        }

        long maxLsn = 0L;
        try (BufferedReader reader = Files.newBufferedReader(logPath, StandardCharsets.UTF_8)) {
            String linha;
            while ((linha = reader.readLine()) != null) {
                if (linha.isBlank()) {
                    continue;
                }
                String[] partes = linha.split(";", 2);
                long lsn = Long.parseLong(partes[0].trim());
                if (lsn > maxLsn) {
                    maxLsn = lsn;
                }
            }
        }
        return maxLsn;
    }

    public synchronized Registo registar(String autor, String canal, String texto) throws IOException {
        long proximoLsn = ultimoLsn + 1;
        Instant agora = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        Registo registo = new Registo(proximoLsn, agora, autor, canal, texto);

        String textoB64 = Base64.getEncoder().encodeToString(texto.getBytes(StandardCharsets.UTF_8));
        String linhaLog = proximoLsn + ";" + agora + ";" + autor + ";" + canal + ";" + textoB64 + "\n";

        ByteBuffer buf = ByteBuffer.wrap(linhaLog.getBytes(StandardCharsets.UTF_8));
        while (buf.hasRemaining()) {
            registosChannel.write(buf);
        }
        registosChannel.force(true);
        this.ultimoLsn = proximoLsn;

        registarEvento("REGISTO", "lsn=" + proximoLsn + " canal=" + canal + " autor=" + autor);

        adicionarAoChunk(registo);

        return registo;
    }

    private synchronized void adicionarAoChunk(Registo registo) throws IOException {
        if (chunkAtual == null) {
            chunkAtual = new Chunk();
            final Chunk chunkAlvo = chunkAtual;
            selagemFuture = scheduler.schedule(() -> {
                try {
                    selarChunk(chunkAlvo);
                } catch (Exception e) {
                    System.err.println("Erro ao selar chunk por timer: " + e.getMessage());
                }
            }, Wire.SELAGEM_MS, TimeUnit.MILLISECONDS);
        }

        chunkAtual.adicionar(registo);

        if (chunkAtual.getTamanhoBruto() >= Wire.CHUNK_MAX_BYTES) {
            selarChunk(chunkAtual);
        }
    }

    private synchronized void selarChunk(Chunk chunkAlvo) throws IOException {
        if (chunkAtual == null || chunkAtual != chunkAlvo || chunkAtual.isSelado() || chunkAtual.estaVazio()) {
            return;
        }
        if (selagemFuture != null) {
            selagemFuture.cancel(false);
            selagemFuture = null;
        }

        Chunk chunkParaSelar = chunkAtual;
        chunkParaSelar.selar();
        chunkAtual = null;

        byte[] plaintextPadded = chunkParaSelar.serializar(Wire.PAD_BYTES);
        try {
            Sealed sealed = ChunkCrypto.seal(plaintextPadded, masterKey);
            Path binPath = seladosDir.resolve(sealed.chunkId() + ".bin");
            Path hdrPath = seladosDir.resolve(sealed.chunkId() + ".hdr");

            Files.write(binPath, sealed.ciphertext());
            Files.writeString(hdrPath, ChunkCrypto.formatHeader(sealed) + "\n", StandardCharsets.UTF_8);

            long lsnInicial = chunkParaSelar.getLsnInicial();
            long lsnFinal = chunkParaSelar.getLsnFinal();
            registarEvento(
                    "SELADO",
                    "chunkId=" + sealed.chunkId()
                            + " lsn=" + lsnInicial + "-" + lsnFinal
                            + " tamanho=" + plaintextPadded.length
            );
            System.out.println("Chunk selado [lsn " + lsnInicial + "-" + lsnFinal
                    + "] id=" + sealed.chunkId() + " plaintext=" + plaintextPadded.length
                    + "B bin=" + sealed.ciphertext().length + "B");
        } catch (java.security.GeneralSecurityException e) {
            throw new IOException("Falha ao cifrar chunk: " + e.getMessage(), e);
        }
    }

    private void registarEvento(String evento, String detalhe) throws IOException {
        String linha = Instant.now() + ";" + evento + ";" + detalhe + "\n";
        Files.writeString(
                eventosPath,
                linha,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND
        );
    }

    @Override
    public void onSlashCommandInteraction(SlashCommandInteractionEvent event) {
        String comando = event.getName();

        // Validação específica para /recuperar
        if (comando.equals("recuperar")) {
            long de = event.getOption("de").getAsLong();
            long ate = event.getOption("ate").getAsLong();
            if (de > ate) {
                event.reply("❌ Intervalo invalido: `de` (" + de + ") é maior que `ate` (" + ate + ").")
                        .setEphemeral(true).queue();
                return;
            }
        }

        // Verificação de permissões para comandos de admin
        if (ADMIN_COMMANDS.contains(comando) && !temPapelAdmin(event.getMember())) {
            event.reply("❌ Nao tens o papel `" + ADMIN_ROLE + "` para executar este comando.")
                    .setEphemeral(true)
                    .queue();
            System.out.println("Comando /" + comando + " recusado a " + event.getUser().getName()
                    + " (sem papel admin)");
            return;
        }

        if (comando.equals("registar")) {
            String autor = event.getUser().getName();
            String texto = event.getOption("texto").getAsString();
            try {
                Registo registo = registar(autor, "registar", texto);
                event.reply("✅ Registado com LSN `" + registo.lsn() + "`.").setEphemeral(true).queue();
            } catch (IOException e) {
                event.reply("❌ Erro ao gravar registo: " + e.getMessage()).setEphemeral(true).queue();
            }
            return;
        }

        if (comando.equals("rajada")) {
            handleRajada(event);
            return;
        }

        // Handler provisório para os restantes comandos
        StringBuilder resposta = new StringBuilder("✅ Comando `/" + comando + "` recebido. Args: ");

        if (event.getOptions().isEmpty()) {
            resposta.append("(nenhum)");
        } else {
            event.getOptions().forEach(opt ->
                    resposta.append(opt.getName()).append("=").append(opt.getAsString()).append(" ")
            );
        }
        resposta.append("— ainda não implementado.");

        event.reply(resposta.toString()).setEphemeral(true).queue();
        System.out.println("Slash Command: /" + comando + " por " + event.getUser().getName());
    }

    private void handleRajada(SlashCommandInteractionEvent event) {
        int n = event.getOption("n", 0, OptionMapping::getAsInt);
        long intervaloMs = event.getOption("intervalo", -1L, OptionMapping::getAsLong);

        if (n <= 0 || intervaloMs < 0) {
            event.reply("❌ Parâmetros inválidos: `n` deve ser > 0 e `intervalo` >= 0.")
                    .setEphemeral(true)
                    .queue();
            return;
        }

        String autor = event.getUser().getName();
        String canal = event.getChannel().getName();

        event.deferReply(true).queue(hook -> CompletableFuture.runAsync(() -> {
            try {
                long primeiroLsn = -1;
                long ultimoLsn = -1;
                String payload = "x".repeat(250);

                for (int i = 1; i <= n; i++) {
                    Registo r = registar(autor, canal, "rajada-" + i + "/" + n + " " + payload);
                    if (i == 1) {
                        primeiroLsn = r.lsn();
                    }
                    ultimoLsn = r.lsn();

                    if (intervaloMs > 0 && i < n) {
                        Thread.sleep(intervaloMs);
                    }
                }

                hook.sendMessage("✅ Rajada concluída: **" + n + "** registos injetados (LSN `"
                        + primeiroLsn + "` a `" + ultimoLsn + "`).").queue();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                hook.sendMessage("⚠️ Rajada interrompida.").queue();
            } catch (Exception e) {
                hook.sendMessage("❌ Erro durante a rajada: " + e.getMessage()).queue();
            }
        }));
    }

    @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        if (event.getAuthor().isBot()) return;
        if (!event.getChannel().getName().equals(ARQUIVO_CHANNEL)) return;

        String texto = event.getMessage().getContentRaw();
        if (texto.isBlank()) return;

        String autor = event.getAuthor().getName();
        String canal = event.getChannel().getName();

        try {
            registar(autor, canal, texto);
            event.getMessage().addReaction(Emoji.fromUnicode("✅")).queue();
        } catch (IOException e) {
            event.getMessage().addReaction(Emoji.fromUnicode("❌")).queue();
        }
    }

    /**
     * Verifica se o membro tem o papel admin-arquivo.
     */
    private boolean temPapelAdmin(Member member) {
        if (member == null) return false;
        return member.getRoles().stream()
                .anyMatch(role -> role.getName().equals(ADMIN_ROLE));
    }
}

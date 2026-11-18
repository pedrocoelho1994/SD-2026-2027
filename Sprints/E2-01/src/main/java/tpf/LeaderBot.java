package tpf;

import net.dv8tion.jda.api.JDA;
import net.dv8tion.jda.api.JDABuilder;
import net.dv8tion.jda.api.entities.Guild;
import net.dv8tion.jda.api.entities.Member;
import net.dv8tion.jda.api.events.interaction.command.SlashCommandInteractionEvent;
import net.dv8tion.jda.api.events.message.MessageReceivedEvent;
import net.dv8tion.jda.api.hooks.ListenerAdapter;
import net.dv8tion.jda.api.interactions.commands.OptionType;
import net.dv8tion.jda.api.interactions.commands.build.Commands;
import net.dv8tion.jda.api.requests.GatewayIntent;

import java.util.Set;

public class LeaderBot extends ListenerAdapter {

    // Comandos que exigem o papel admin-arquivo
    private static final Set<String> ADMIN_COMMANDS = Set.of(
            "config", "replica-token", "replica-remover", "rajada"
    );

    private static final String ADMIN_ROLE = "admin-arquivo";
    private static final String ARQUIVO_CHANNEL = "arquivo";

    public static void main(String[] args) throws InterruptedException {
        // 1. Ler as variáveis de ambiente estritamente do launch.json
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

        // 2. Construir o JDA com os Intents necessários
        JDA jda = JDABuilder.createDefault(token)
                .enableIntents(GatewayIntent.MESSAGE_CONTENT, GatewayIntent.GUILD_MESSAGES)
                .addEventListeners(new LeaderBot())
                .build();

        jda.awaitReady();
        System.out.println("Bot ligado como " + jda.getSelfUser().getName());

        // 3. Registar os comandos no servidor de teste (Guild Commands)
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

        // Handler provisório: responde com os argumentos recebidos
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

     @Override
    public void onMessageReceived(MessageReceivedEvent event) {
        // Ignorar mensagens de bots
        if (event.getAuthor().isBot()) return;

        // Verificar se a mensagem é no canal #arquivo
        if (event.getChannel().getName().equals(ARQUIVO_CHANNEL)) {
            
            // 1. Imprimir no terminal do VS Code
            System.out.println("Mensagem detetada em #arquivo por " + event.getAuthor().getEffectiveName());
            
            // 2. Responder no Discord com o nome do utilizador no fim
            String nomeDoUtilizador = event.getMember().getEffectiveName();
            event.getChannel()
                    .sendMessage("📥 Recebi a tua mensagem dentro do arquivo " + nomeDoUtilizador)
                    .queue();
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
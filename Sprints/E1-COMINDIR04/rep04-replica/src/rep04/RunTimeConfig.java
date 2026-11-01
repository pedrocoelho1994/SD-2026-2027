package rep04;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Properties;

/** Carrega a configuração comum, procurando-a no contexto de execução. */
/** Localiza e carrega a configuração comum dos processos. */
final class RuntimeConfig {
    private static final String FILE_NAME = "runtime.properties";

    private RuntimeConfig() { }

    /** Localiza e lê {@code runtime.properties} em UTF-8. */
    static Properties load() throws IOException {
        Path file = findFile();
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(file, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return properties;
    }

    /** Obtém uma propriedade não vazia, com fallback explícito. */
    static String value(Properties properties, String key, String fallback) {
        String value = properties.getProperty(key, fallback).trim();
        if (value.isEmpty()) {
            throw new IllegalArgumentException("Configuracao vazia: " + key);
        }
        return value;
    }

    /** Procura primeiro o caminho explícito e depois diretórios ancestrais. */
    private static Path findFile() throws IOException {
        String explicitPath = System.getProperty("comindir04.config");
        if (explicitPath != null && !explicitPath.isBlank()) {
            Path file = Path.of(explicitPath).toAbsolutePath().normalize();
            if (Files.isRegularFile(file)) {
                return file;
            }
            throw new IOException("Ficheiro de configuracao nao encontrado: " + file);
        }

        List<Path> roots = new ArrayList<>();
        addAncestors(roots, Path.of(System.getProperty("user.dir")));
        try {
            Path classes = Path.of(RuntimeConfig.class.getProtectionDomain()
                    .getCodeSource().getLocation().toURI());
            addAncestors(roots, classes);
        } catch (Exception e) {
            throw new IOException("Nao foi possivel localizar runtime.properties.", e);
        }

        for (Path root : new LinkedHashSet<>(roots)) {
            Path direct = root.resolve(FILE_NAME);
            if (Files.isRegularFile(direct)) {
                return direct;
            }
            Path repository = root.resolve("Sprints/E1-COMINDIR04").resolve(FILE_NAME);
            if (Files.isRegularFile(repository)) {
                return repository;
            }
        }
        throw new IOException("runtime.properties nao encontrado. Execute a partir do "
                + "COMINDIR04 ou defina -Dcomindir04.config=<caminho>.");
    }

    /** Adiciona um caminho e todos os seus pais sem alterar o sistema de ficheiros. */
    private static void addAncestors(List<Path> roots, Path path) {
        Path current = path.toAbsolutePath().normalize();
        while (current != null) {
            roots.add(current);
            current = current.getParent();
        }
    }
}
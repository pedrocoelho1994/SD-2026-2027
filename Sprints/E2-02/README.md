# TPF · Líder (LeaderBot)

Bot Discord que serve de líder para o arquivo distribuído zero-knowledge.

Este é o **Sprint E2-02**: registo durável (`registos.log` com `fsync`), chunking por tamanho/tempo, serialização com padding e cifra autenticada (`HKDF-SHA256` + `AES-256-GCM`).

## Pré-requisitos

- Java 17+
- Maven
- Uma aplicação Discord criada em https://discord.com/developers/applications

## Configuração

### 1. Token do bot

O token **nunca** deve ir para o repositório. Configura-o como variável de ambiente:

- **VS Code**: edita `.vscode/launch.json` e preenche `"DISCORD_TOKEN"`.
- **Terminal** (Linux/macOS): `export DISCORD_TOKEN="o-teu-token"`
- **Terminal** (Windows): `set DISCORD_TOKEN=o-teu-token`

### 2. ID do servidor

Copia o ID do teu servidor de teste (Modo Developer > botão direito no servidor > Copiar ID) e define:

- **VS Code**: `"DISCORD_GUILD_ID"` no `launch.json`.
- **Terminal**: `export DISCORD_GUILD_ID="o-id-do-servidor"`

### 3. Estrutura do servidor Discord

- Canal de texto `#arquivo`
- Cargo `admin-arquivo` (atribuído aos membros do grupo)

### 4. Parâmetros (`Wire` via `-Dtpf.*`)

| Propriedade | Omissão | Descrição |
| --- | --- | --- |
| `-Dtpf.CHUNK_MAX_BYTES` | `65536` | Tamanho máximo bruto do chunk antes de selar |
| `-Dtpf.PAD_BYTES` | `4096` | Alinhamento por padding dos bytes em claro do chunk |
| `-Dtpf.SELAGEM_MS` | `5000` | Tempo máximo desde o 1.º registo do chunk até selar |

## Ficheiros locais (`dados-lider/`)

- `dados-lider/master.key` — chave mestra de 32 bytes (`SecureRandom`, leitura só para o dono; arranca apenas se tiver exatamente 32 bytes).
- `dados-lider/registos.log` — WAL com `lsn;instante;autor;canal;textoBase64`, gravado com `FileChannel.force(true)` (`fsync`) antes da resposta.
- `dados-lider/eventos-lider.txt` — registo de eventos `instante;evento;detalhe` (`REGISTO` e `SELADO`).
- `dados-lider/selados/<chunkId>.bin` e `<chunkId>.hdr` — chunks selados e cifrados, identificados pelo SHA-256 em hexadecimal de `.bin`.

## Formato binário do Chunk e do `.hdr`

### Chunk em claro (antes da cifra, big-endian)

1. **Cabeçalho:** `4 bytes` (`int`) — `numRegistos` ($N$).
2. **Corpo:** $N$ registos sequenciais, cada um com:
   - `lsn` (`8 bytes`, `long`)
   - `instante` (`8 bytes`, `long` em milissegundos desde epoch UTC)
   - `autor` (`4 bytes` `int` comprimento + bytes UTF-8)
   - `canal` (`4 bytes` `int` comprimento + bytes UTF-8)
   - `texto` (`4 bytes` `int` comprimento + bytes UTF-8)
3. **Padding:** bytes `0x00` no final até ao menor múltiplo de `PAD_BYTES` (`4096`). Na leitura inversa, leem-se exatamente $N$ registos e os restantes bytes de padding são ignorados.

### Cifra e ficheiro `.hdr`

- **Derivação de chave:** `DEK = HKDF-SHA256(MK, salt, info = "tpf-chunk-v1")` (Extract com `salt` como chave e `MK` como dados; Expand de 1 bloco com `PRK` como chave e `info || 0x01`).
- **Cifra:** `AES-256-GCM` com `nonce` aleatório de 12 bytes, tag de 128 bits e `AAD = "tpf-v1" || keyVersion (4B big-endian) || salt (32B) || nonce (12B)`.
- **`<chunkId>.bin`:** texto cifrado com tag GCM de 16 bytes no final; `chunkId` = `SHA-256` em hexadecimal (64 caracteres) deste conteúdo.
- **`<chunkId>.hdr`:** linha de texto UTF-8 no formato `keyVersion;saltBase64;nonceBase64` (ex.: `1;<salt_em_Base64>;<nonce_em_Base64>`).

## Arrancar e testar

```bash
mvn test
mvn compile
mvn exec:java -Dexec.mainClass="tpf.Main"
```

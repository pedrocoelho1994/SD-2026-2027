# TPF · Líder (LeaderBot)

Bot Discord que serve de líder para o arquivo distribuído zero-knowledge.

Este é o **Sprint E2-01**: configuração do Discord + setup da aplicação + handling de comandos (provisório).

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

## Arrancar o líder

```bash
mvn compile
mvn exec:java -Dexec.mainClass="tpf.Main"
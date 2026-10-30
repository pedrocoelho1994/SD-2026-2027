# COMINDIR04 · Deteção de Falhas do Líder por Heartbeat e Envio em Lote

Sistema distribuído em Java que deteta a falha do líder de cada partição através de
**heartbeats** e entrega os registos em **lotes** (LOTEs), usando **IPFS PubSub** como
canal de comunicação.

---

## 1. O que o sistema faz

- Divide os sensores por **N partições** (por omissão 3).
- Cada partição tem **um líder** que atribui `seq`, relógio de Lamport (`L`) e relógio
  vetorial (`V`) a cada leitura.
- O líder **não publica cada registo**: acumula-os e publica um **LOTE a cada 5 segundos**.
- O LOTE serve ao mesmo tempo para:
  - entregar os dados acumulados nesse intervalo;
  - provar que o líder está **vivo** (heartbeat), mesmo quando vai vazio.
- Os participantes (réplicas, processador de alarmes e produtor) mantêm um **detetor de
  falhas** que marca o líder como **SUSPEITO** se ficar mais de **15 s** sem receber
  LOTEs.
- Se faltarem registos, a réplica pede-os ao líder por **TCP** (recuperação).

---

## 2. Processos

| Processo | Papel |
|---|---|
| **Produtor** | Lê da consola (`S01 21.0`, `rajada 300 10`, `sair`) e publica `IN` no tópico da partição do sensor. |
| **Líder p\<k\>** | Atribui `seq`, `L`, `V`; grava em `lider-p<k>.txt`; publica um `LOTE` a cada 5 s; serve recuperação TCP na porta `7000+k`. |
| **Réplica** | Subscreve 2 partições; aplica registos pela ordem do `seq`; vigia o líder; pede por TCP os que faltam. |
| **Processador de Alarmes** | Observa todas as partições; se `temperatura ≥ 28`, publica um novo `IN` com nome `ALARM-<sensor>` na partição de destino. |

---

## 3. Fluxo da aplicação (6 passos)


### Passo 1 — Produtor envia leitura

O produtor escreve uma mensagem no tópico `rep04-in-p<k>`, onde `k` é a partição
calculada por `floorMod(sensor.hashCode(), N)`:


O produtor **não é um evento causal**: usa sempre `L=0` e `V` todo a zero.

### Passo 2 — Líder guarda o registo

O líder, numa secção crítica, faz:

1. `L = max(L, L_recebido) + 1`
2. `V = merge(V, V_recebido)` e `V[k]++`
3. `seq++` e verifica `V[k] == seq`
4. `append + fsync` em `lider-p<k>.txt`
5. adiciona o registo a `pendentes`

**Ainda não publica nada.** O registo só sai no próximo LOTE.

### Passo 3 — LOTE a cada 5 segundos (dados + heartbeat)

A cada `Δ = 5000 ms`, o líder:

1. Fecha o lote: `lote = pendentes`, `pendentes = []`, `hb++`, lê `ultimoSeq`.
2. Publica no tópico `rep04-p<k>`:

3. Regista uma linha em `hb-p<k>.txt`:
   `inc;hb;instante;ultimoSeq;n;bytes`

**Mesmo vazio, o LOTE é enviado** — é o sinal de vida do líder.

### Passo 4 — Réplicas aplicam e vigiam

Cada réplica subscreve 2 partições. Para cada LOTE recebido:

1. `onHeartbeat(inc, hb)` → atualiza o detetor de falhas.
2. Aplica os registos pela ordem do `seq` (duplicados ignorados).
3. Se `ultimo < ultimoSeq` → pede os registos em falta por TCP (`PEDIDO de ate` → linhas → `FIM`).

Em paralelo, uma thread verifica a cada 1 s se o líder está em silêncio há mais de
`3 × Δ = 15 s`. Se sim → **SUSPEITO** (avisa, mas continua a tentar aplicar).

### Passo 5 — Processador cria alarmes

O processador subscreve todas as partições. Para cada registo com `temperatura ≥ 28`:

1. Cria `alarmSensor = "ALARM-" + sensor`.
2. Calcula `target = partitioner.of(alarmSensor)`.
3. Publica um novo `IN` no tópico `rep04-in-p<target>`.

O alarme **herda `L` e `V`** do processador (elo causal entre partições).
Se o líder de destino estiver `SUSPEITO`, imprime `AVISO` mas publica na mesma.

### Passo 6 — O ciclo fecha

O novo `IN` (`ALARM-...`) é tratado pelo líder de destino como **qualquer outro registo**:
recebe `seq`, `L`, `V`, é gravado e sai no próximo LOTE.

**Latência máxima de um alarme:** `Δ` (espera no líder de origem) + `Δ` (espera no
líder de destino) = **2 × Δ = 10 s**.

---

## 4. Diagrama simples

```mermaid
flowchart LR
    A["1. Produtor<br/>envia IN"] --> B["2. Líder<br/>guarda + espera 5s"]
    B --> C["3. LOTE a cada 5s<br/>(dados + heartbeat)"]
    C --> D["4. Réplicas<br/>aplicam + detetam falhas"]
    C --> E["5. Processador<br/>cria ALARM-*"]
    E --> F["6. Novo IN<br/>volta ao passo 2"]

    


# 05 — Especificação do protocolo de sinalização v2

Referência normativa. Se uma spec divergir daqui, este documento vence.

## 0. Envelope

Inalterado em forma, novo em conteúdo:

```json
{ "v": 1, "op": "<namespace>.<action>", "data": { } }
```

O campo `v` permanece `1` **para sempre**. Ele identifica o formato do
envelope, não do protocolo. A versão do protocolo é negociada no handshake
(§1). Mudar `v` quebraria clientes antigos no parse do envelope, o que é
exatamente o que precisamos evitar.

Schema em `protocol/websocket-envelope.schema.json` fica como está.

## 1. Handshake e negociação de versão

### 1.1 `auth.hello` (C→S) — estendido

```json
{
  "token": "<string, obrigatório>",
  "protocol_version": 2,
  "client_version": "1.4.0",
  "client_platform": "windows"
}
```

| Campo | Tipo | Obrigatório | Notas |
|---|---|---|---|
| `token` | string | sim | inalterado |
| `protocol_version` | inteiro | não | ausente significa `1` |
| `client_version` | string | não | SemVer da build; ausente significa `"unknown"` |
| `client_platform` | string | não | `"windows"`, `"dev"`; ausente significa `"unknown"` |

Rust:

```rust
#[derive(Debug, Deserialize)]
pub struct AuthHello {
    pub token: String,
    #[serde(default = "default_protocol_version")]
    pub protocol_version: u8,
    #[serde(default)]
    pub client_version: Option<String>,
    #[serde(default)]
    pub client_platform: Option<String>,
}
fn default_protocol_version() -> u8 { 1 }
```

O servidor guarda `negotiated = min(protocol_version_do_cliente, MAX_SERVER_PROTOCOL)`
por conexão, onde `MAX_SERVER_PROTOCOL = 2`.

### 1.2 `auth.ok` (S→C) — estendido

```json
{
  "user_id": "<uuid>",
  "username": "<string>",
  "display_name": "<string>",
  "livekit_url": "<string|null>",
  "protocol_version": 2,
  "server_version": "0.2.0",
  "features": ["voice.room.v2", "voice.hints", "client.logs"]
}
```

| Campo | Tipo | Notas |
|---|---|---|
| `protocol_version` | inteiro | a versão negociada; um cliente v2 que receber `1` cai para o dialeto v1 |
| `server_version` | string | `CARGO_PKG_VERSION` |
| `features` | array de string | capacidades opcionais; cliente ignora as que não conhece |

Nomes de feature definidos na v2.0:

| Feature | Significa |
|---|---|
| `voice.room.v2` | servidor emite `voice.room.state` e `voice.room.delta` |
| `voice.hints` | servidor aceita `voice.presence.hint` |
| `client.logs` | endpoint `POST /api/client-logs` disponível |

## 2. Estado de sala de voz — v2

Duas ops substituem `voice.rooms` e `voice.roster` para clientes v2. As v1
continuam sendo emitidas para clientes v1 (§6).

### 2.1 `voice.room.state` (S→C) — snapshot

Enviado quando: a conexão autentica, o cliente pede (`voice.room.request`), ou
o servidor detecta que o cliente pode estar dessincronizado.

```json
{
  "full": true,
  "channel_ids": ["<uuid>"],
  "rooms": [
    {
      "channel_id": "<uuid>",
      "version": 42,
      "participants": [
        {
          "user_id": "<uuid>",
          "participant_sid": "PA_xxx",
          "muted": false,
          "deafened": false,
          "is_bot": false,
          "provisional": false,
          "joined_at": "2026-09-02T18:04:11Z"
        }
      ],
      "tracks": [
        {
          "track_sid": "TR_xxx",
          "owner": "<uuid>",
          "owner_sid": "PA_xxx",
          "source": "screen_share",
          "muted": false
        }
      ]
    }
  ]
}
```

Contrato campo a campo:

| Campo | Tipo | Nulo? | Semântica |
|---|---|---|---|
| `full` | booleano | não | sempre `true` nesta op; existe para simetria com o delta |
| `channel_ids` | array de uuid | sim | ausente em um snapshot completo; presente com o escopo exato de uma resposta a `voice.room.request` filtrada. O cliente substitui somente esses canais e remove os ids do escopo que não vierem em `rooms`. |
| `rooms` | array | não | **apenas** canais com ao menos um participante, visíveis ao usuário |
| `rooms[].channel_id` | uuid | não | id do canal de voz |
| `rooms[].version` | inteiro sem sinal | não | monotônico crescente por canal; nunca reinicia enquanto o processo vive |
| `participants[].user_id` | uuid | não | identidade Tupi |
| `participants[].participant_sid` | string | sim | `null` quando `provisional` é `true` |
| `participants[].muted` | booleano | não | reportado pelo próprio usuário via `call.state.update` |
| `participants[].deafened` | booleano | não | idem |
| `participants[].is_bot` | booleano | não | `true` só para `MUSIC_BOT_ID` |
| `participants[].provisional` | booleano | não | `true` = anunciado pelo cliente, não confirmado pelo LiveKit |
| `participants[].joined_at` | string RFC3339 | não | quando entrou na projeção |
| `tracks[].track_sid` | string | não | SID do LiveKit; chave primária da track |
| `tracks[].owner` | uuid | não | dono |
| `tracks[].owner_sid` | string | sim | sid da sessão do dono; `null` se provisório |
| `tracks[].source` | enum string | não | `microphone`, `camera`, `screen_share`, `screen_share_audio`, `music` |
| `tracks[].muted` | booleano | não | mute da publicação, reportado pelo LiveKit; default `false` |

**Importante:** `version` de uma sala **não** é comparável com o de outra.
Cada canal tem seu próprio contador.

Quando o processo do servidor reinicia, `version` recomeça em 1. O cliente
precisa tratar um `version` **menor** que o local como "servidor reiniciou,
aceite o snapshot como verdade" — nunca como mensagem obsoleta. Isso é
explícito porque é uma armadilha clássica.

### 2.2 `voice.room.delta` (S→C) — mudança incremental

```json
{
  "channel_id": "<uuid>",
  "version": 43,
  "previous_version": 42,
  "participants_added": [ /* mesma forma de participants[] */ ],
  "participants_removed": ["<uuid>"],
  "participants_updated": [ /* mesma forma de participants[] */ ],
  "tracks_added": [ /* mesma forma de tracks[] */ ],
  "tracks_removed": ["TR_xxx"],
  "reason": "webhook.participant_joined"
}
```

| Campo | Tipo | Obrigatório | Semântica |
|---|---|---|---|
| `channel_id` | uuid | sim | |
| `version` | inteiro | sim | versão **após** aplicar este delta |
| `previous_version` | inteiro | sim | versão antes; o cliente exige `previous_version == versãoLocal` |
| `participants_added` | array | sim, pode ser vazio | |
| `participants_removed` | array de uuid | sim, pode ser vazio | |
| `participants_updated` | array | sim, pode ser vazio | substitui a entrada inteira daquele `user_id` |
| `tracks_added` | array | sim, pode ser vazio | |
| `tracks_removed` | array de string | sim, pode ser vazio | por `track_sid` |
| `reason` | string | sim | ver tabela abaixo; é dado de diagnóstico, o cliente não decide nada com ele |

Valores válidos de `reason`:

```
webhook.participant_joined   webhook.participant_left
webhook.track_published      webhook.track_unpublished
reconcile.added              reconcile.removed          reconcile.track_sync
ws.presence_hint             ws.state_update            ws.track_hint
admin.disconnect_member      channel.deleted            provisional.expired
```

**Regra de aplicação no cliente (normativa):**

```
ao receber delta D para o canal C:
  se não tenho estado local de C:
      ignorar D e enviar voice.room.request {channel_ids: [C]}
  senão se D.previous_version == local[C].version:
      aplicar D; local[C].version = D.version
  senão se D.version <= local[C].version:
      ignorar D            // reentrega duplicada
  senão:
      // lacuna detectada
      enviar voice.room.request {channel_ids: [C]}
      marcar C como "aguardando snapshot"; ignorar deltas de C até chegar
```

Ordem de aplicação dentro de um delta: `participants_removed`,
`tracks_removed`, `participants_added`, `participants_updated`, `tracks_added`.
Fixar a ordem evita divergência entre implementações.

### 2.3 `voice.room.request` (C→S)

```json
{ "channel_ids": ["<uuid>"] }
```

`channel_ids` vazio ou ausente significa "todos os canais visíveis". A resposta
é sempre um `voice.room.state` com `full: true`.

Rate limit: no máximo 5 por conexão por 10 segundos. Excedente responde
`error` com `code: "rate_limited"` e é descartado, sem desconectar.

## 3. Dicas do cliente (não autoritativas)

### 3.1 `voice.presence.hint` (C→S)

Substitui `voice.presence.enter` para clientes v2. O nome muda de propósito:
deixa claro que é uma dica, não uma afirmação de estado (INV-A1).

```json
{
  "channel_id": "<uuid>",
  "state": "joining",
  "participant_sid": "PA_xxx"
}
```

| Campo | Tipo | Obrigatório | Semântica |
|---|---|---|---|
| `channel_id` | uuid | sim | |
| `state` | enum | sim | `joining` ou `leaving` |
| `participant_sid` | string | não | quando o cliente já conhece o sid da sua sessão LiveKit |

Efeito no servidor:

- `joining`: valida membership e tipo do canal. Insere participante com
  `provisional: true` se ainda não houver um confirmado. Se `participant_sid`
  vier preenchido, marca `provisional: false` (o cliente só sabe o sid depois de
  `room.connect()` ter sucedido, o que é prova de conexão real).
  Emite delta com `reason: "ws.presence_hint"`.
- `leaving`: **remove apenas se o participante for provisório.** Um participante
  confirmado pelo LiveKit não sai por dica de cliente (INV-A1); o servidor
  registra a intenção e agenda um reconcile daquele canal em 2 s, para
  confirmar rapidamente a saída real.

Essa assimetria é deliberada e é o coração da correção do sintoma 1: entrar
pode ser otimista (o custo de errar é uma linha a mais por até 10 s), sair não
pode (o custo de errar é o fantasma silencioso que o usuário relatou).

### 3.2 `voice.track.hint` (C→S)

Substitui `voice.track.published` / `voice.track.unpublished`.

```json
{
  "channel_id": "<uuid>",
  "track_sid": "TR_xxx",
  "source": "screen_share",
  "state": "published"
}
```

| Campo | Tipo | Obrigatório | Semântica |
|---|---|---|---|
| `channel_id` | uuid | sim | |
| `track_sid` | string | sim | **obrigatório na v2**, ao contrário da v1 |
| `source` | enum | sim | `camera`, `screen_share`, `screen_share_audio` |
| `state` | enum | sim | `published` ou `unpublished` |

Validação obrigatória no servidor (INV-F1):

1. o remetente precisa ser participante confirmado ou provisório do canal;
2. em `unpublished`, a track referida por `track_sid` precisa ter
   `owner == remetente`, senão a op é rejeitada com
   `error { code: "forbidden" }` e nada muda.

### 3.3 `call.state.update` (C→S) — inalterado

Continua sendo a única op em que o cliente é autoridade, porque mute e deafen
são estado exclusivamente do Tupi. Payload idêntico à v1
(`server/src/ws/protocol.rs:275-280`). O servidor passa a emitir a mudança
como `voice.room.delta` com `participants_updated` e
`reason: "ws.state_update"`, além de manter o `call.state.update` de saída para
clientes v1.

## 4. Operações administrativas

### 4.1 `voice.move_member` (C→S) — inalterado em forma

Payload igual à v1. Muda apenas o comportamento interno: após enviar
`voice.moved` ao alvo, o servidor agenda um reconcile do canal de origem e do
canal de destino em 3 s, para não depender só do webhook.

### 4.2 `voice.disconnect_member` (C→S) — inalterado em forma

Payload igual à v1. Muda o comportamento: o servidor chama
`RemoveParticipant` no LiveKit e **não** remove localmente por conta própria
(INV-A1). A remoção chega pelo webhook `participant_left`; se em 3 s não
chegar, um reconcile daquele canal é forçado.

### 4.3 `voice.moved` e `voice.disconnected` (S→C) — inalterados

## 5. Webhook do LiveKit — contrato de entrada estendido

`POST /api/livekit/webhook`. O servidor passa a decodificar campos que hoje
ignora:

```rust
#[derive(Debug, Deserialize)]
pub struct WebhookEvent {
    pub event: String,
    #[serde(default)] pub id: Option<String>,
    #[serde(default, rename = "createdAt")] pub created_at: Option<i64>,
    pub room: Option<Room>,
    pub participant: Option<Participant>,
    pub track: Option<Track>,
}

#[derive(Debug, Deserialize)]
pub struct Participant {
    pub identity: String,
    #[serde(default)] pub sid: Option<String>,
    #[serde(default)] pub state: Option<String>,
    #[serde(default)] pub permission: Option<ParticipantPermission>,
}

#[derive(Debug, Deserialize)]
pub struct ParticipantPermission {
    #[serde(default)] pub hidden: bool,
    #[serde(default, rename = "canPublish")] pub can_publish: bool,
}

#[derive(Debug, Deserialize)]
pub struct Track {
    pub source: String,
    #[serde(default)] pub sid: Option<String>,
    #[serde(default)] pub muted: bool,
}
```

Eventos tratados e efeito na v2:

| Evento | Efeito |
|---|---|
| `participant_joined` | se `permission.hidden`, ignora (INV-B3). Senão insere ou confirma com `sid`; delta `webhook.participant_joined` |
| `participant_left` | se o `sid` não bater com o registrado, ignora e loga `ignored_stale` (INV-B2). Senão remove participante e todas as tracks dele |
| `track_published` | insere track por `track_sid`; delta `webhook.track_published` |
| `track_unpublished` | remove track por `track_sid`; se o sid não existir, loga `ignored_unknown` e não faz nada |
| `room_finished` | **não** apaga nada. Agenda reconcile daquele canal (INV-A1, RC-04) |
| `room_started` | ignora |
| `track_muted` / `track_unmuted` | atualiza `tracks[].muted`; delta |
| qualquer outro | ignora, com log em `debug` |

Deduplicação: uma janela LRU com os últimos 512 `(event, id)` processados. Uma
reentrega é descartada com log `ignored_duplicate`. Se `id` vier ausente, a
chave é `(event, room, participant_sid, track_sid, created_at)`.

Rejeição por idade: eventos com `created_at` mais de 60 s no passado são
processados normalmente **mas** disparam um reconcile do canal, porque indicam
atraso significativo do webhook.

## 6. Compatibilidade com clientes v1

Enquanto `negotiated == 1`, a conexão recebe **exatamente** o que recebe hoje:

| Op v2 | Equivalente v1 emitido para conexões v1 |
|---|---|
| `voice.room.state` | `voice.rooms` com o mesmo conteúdo, sem `version`, sem `provisional`, `streams[]` no formato antigo |
| `voice.room.delta` | `voice.roster` do canal afetado, com a lista completa daquele canal |

E aceita as ops v1 de entrada:

| Op v1 recebida | Tratada como |
|---|---|
| `voice.presence.enter` | `voice.presence.hint { state: "joining" }` sem `participant_sid` |
| `voice.presence.leave` | `voice.presence.hint { state: "leaving" }` |
| `voice.track.published` | `voice.track.hint { state: "published" }`; se `track_sid` ausente, a dica é **descartada** com log (não dá para endereçar por SID sem SID) |
| `voice.track.unpublished` | `voice.track.hint { state: "unpublished" }`; mesma regra |
| `voice.rooms.request` | `voice.room.request { channel_ids: [] }` |

Nota importante sobre a perda funcional para clientes v1: a partir da v2, um
`voice.presence.leave` de cliente v1 **não** remove mais um participante
confirmado. O cliente v1 depende disso para limpar a sidebar rapidamente ao
sair. Na prática o reconcile agendado em 2 s cobre o caso, então o cliente v1
vê a linha sumir em até 2 s em vez de instantaneamente. É uma regressão de
latência aceita e deliberada em troca de eliminar o fantasma (ver
`08-rollout-plan.md` §4).

### Mapeamento de `StreamDto` v1 a partir de tracks v2

Clientes v1 esperam `streams[]` com `{stream_id, owner, kind, label, has_audio, msid}`
(`server/src/ws/protocol.rs:352-365`). O servidor deriva isso das tracks v2:

- agrupa tracks do mesmo dono por `kind`, onde `screen_share` e
  `screen_share_audio` viram um único `kind: "screen"`;
- `stream_id` = UUID v5 determinístico de `(channel_id, owner, kind)`, com o
  namespace fixo `6f0c2f8c-8e40-4a3e-9d2f-1c0a1b2c3d4e`. Determinístico é
  essencial: o mesmo compartilhamento precisa ter o mesmo `stream_id` entre
  broadcasts, senão clientes v1 tratam como stream novo a cada evento;
- `msid` = `track_sid` da track de vídeo daquele grupo;
- `has_audio` = existe track `screen_share_audio` do mesmo dono;
- `label` = `null`.

## 7. Erros

Forma inalterada (`server/src/ws/protocol.rs:46-51`). Códigos usados no caminho
de voz na v2:

| `code` | Quando | Cliente deve |
|---|---|---|
| `forbidden` | sem permissão para a ação | mostrar mensagem |
| `not_found` | canal ou track inexistente | ignorar silenciosamente |
| `validation_error` | payload inválido | logar, não mostrar |
| `rate_limited` | excedeu limite de `voice.room.request` | esperar e não reenviar |
| `channel_full` | canal lotado ao pedir token | mostrar mensagem |
| `unknown_op` | op desconhecida | ignorar silenciosamente (já é o comportamento, `App.tsx:1542`) |

## 8. REST — mudanças

### 8.1 `POST /api/livekit/token` — inalterado em forma

Passa a recusar com `409` e corpo `{"code":"channel_full","message":"..."}`
quando o canal já tem o número máximo de participantes confirmados não-bot e o
solicitante ainda não está lá (INV-F2). Limite: 10, alinhado ao
`room.max_participants: 12` do LiveKit, que deixa margem para bot e um
espectador.

### 8.2 `GET /api/debug/voice` — novo

Autenticado, apenas para owner da comunidade. Devolve o `VoiceRegistry` inteiro
serializado, com `version` por canal, `reconciled_at` e contadores de eventos.
Formato definido em SPEC-002.

### 8.3 `POST /api/client-logs` — novo

Autenticado. Recebe o ring buffer de diagnóstico do cliente. Formato em
SPEC-014.

## 9. Tabela consolidada de ops de voz na v2

| op | Direção | Autoridade | Versão mínima |
|---|---|---|---|
| `auth.hello` | C→S | — | 1 (campos novos opcionais) |
| `auth.ok` | S→C | — | 1 (campos novos opcionais) |
| `voice.room.state` | S→C | servidor (projeção do LiveKit) | 2 |
| `voice.room.delta` | S→C | servidor (projeção do LiveKit) | 2 |
| `voice.room.request` | C→S | — | 2 |
| `voice.presence.hint` | C→S | dica | 2 |
| `voice.track.hint` | C→S | dica | 2 |
| `call.state.update` | bidirecional | cliente (só o próprio) | 1 |
| `voice.move_member` | C→S | owner | 1 |
| `voice.disconnect_member` | C→S | owner, ou membro para o bot | 1 |
| `voice.moved` | S→C | servidor | 1 |
| `voice.disconnected` | S→C | servidor | 1 |
| `voice.rooms` | S→C | servidor | 1 apenas |
| `voice.roster` | S→C | servidor | 1 apenas |
| `voice.presence.enter` / `leave` | C→S | dica (traduzida) | 1 apenas |
| `voice.track.published` / `unpublished` | C→S | dica (traduzida) | 1 apenas |
| `voice.rooms.request` | C→S | — | 1 apenas |
| `stream.publish` / `unpublish` | C→S | só o bot, `kind: "music"` | 1 |

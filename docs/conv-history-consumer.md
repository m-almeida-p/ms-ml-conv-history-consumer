# conv-history-consumer

A Camel Kafka consumer, built in Karavan, that **builds the conversation history** from the events topic: the
same three tables, with the same rules, as the reference service
[`ms-ml-conv-history-bs`](../../ms-ml-conv-history-bs). It also enforces the Kafka schema and writes every
poison message to a dead-letter log.

Project folder: [`conv-history-consumer/`](../conv-history-consumer)

## What it does

It reads records from the events topic and, for each one, writes the event to three tables. A record it cannot store
goes to the dead-letter log. The diagrams, from the big picture down to the steps, are in [design.md](design.md): start
with its [context diagram](design.md#level-1-context).

The Designer draws **three routes**, and the numbers below are the same there, in the diagrams and in the notes of
each step:

| Route | Steps |
|---|---|
| **1 Receive** | 1.1 Decode and validate the record (schema id, registry schema, contract, projection) → 1.2 Route by kind of record (a tombstone is skipped, any other record goes to route 2) → 1.3 Commit the offset, only now |
| **2 Store** | 2.1 Write inbox, message and summary with one statement in one transaction → 2.2 Decide stored, duplicate or conflict → 2.3 Log the outcome |
| **3 Dead-letter** | 3.1 Name the reason if no rule did → 3.2 Write the record to the dead-letter log → 3.3 Commit the offset |

Records that fail validation or that the store refuses go to route 3. A database or registry outage is retried
forever and the offset does not move. Anything unexpected is retried 3 times, then goes to route 3 as `reason=unexpected`.

The tables live in the schema **`camel_history`** of the cluster's Postgres, apart from the original consumer's
`public.conversation_*` tables, so both can run and be compared. Their DDL is the migration folder
`history/camel_conversation` of `ml-genai-agents-bs-testing`
(`make migrations/up SERVICE=history FLOW=camel_conversation`).

## Project files

| File | Role |
|---|---|
| `consume-conversation-events.camel.yaml` | Route 1, Receive: Kafka → decode → route → commit. |
| `store-conversation-history.camel.yaml` | Route 2, Store: the SQL statement, the outcome. Called as `direct:store`. |
| `handle-errors-and-dead-letters.camel.yaml` | The error rules, and route 3, Dead-letter (`direct:deadLetter`). |
| `PROJECT.md` | The legend, the numbering and the vocabulary, shown with the project in Karavan. |
| `ConversationEventDecoder.java` | Schema id check, registry schema validation, contract, projection. |
| `StoreOutcome.java` | Reads the store statement's result: `stored`, `duplicate`, or a conflict. |
| `OffsetCommit.java` | Commits the offset by hand (auto commit is off). |
| `StoreRefusal.java` | Names a store refusal for the dead-letter log: reason `store-rejected`, field `sqlstate-<code>`, found in the cause chain. |
| `PoisonRecordException.java` | The exception for a record that can never be stored. |
| `application.properties` | Camel settings, dependencies, the datasource and the `app.*` settings. |

The four helpers are Java files because decoding bytes and validating JSON is clearer in code than in YAML. Each
registers itself with `@BindToRegistry`, so the route refers to them by name (`decoder`, `storeOutcome`,
`offsetCommit`, `storeRefusal`).

## Settings

In `application.properties`. Any `app.*` value can be overridden by an environment variable (`app.topic` →
`APP_TOPIC`), which is how the local runner and the tests use a throwaway topic.

| Setting | Default | Meaning |
|---|---|---|
| `app.topic` | `yape.public.conversation_events` | Topic to read. |
| `app.kafka.brokers` | `kafka:29092` | The cluster's internal listener (from the host: `localhost:9092`). |
| `app.kafka.groupId` | `ms-ml-conv-history-consumer` | Different from the original service's group (`ms-ml-conv-history-bs`) and from the group of the consumer in `ms-ml-camel-conv-history-bs`, so none of them compete. A new group reads the topic from the start; that is safe, because the inbox refuses a duplicate. |
| `app.schema.registryUrl` | `http://schema-registry:8081` | Where schemas are read. |
| `app.schema.allowedIds` | `2` | Schema ids accepted, comma separated. |
| `app.schema.requireFraming` | `true` | `false` also accepts plain JSON, as the original does. |
| `app.db.url` / `app.db.user` | `jdbc:postgresql://db:5432/postgres?currentSchema=camel_history` / `postgres` | Target. The cluster trusts local connections: no password. |

The project's port is `8081` on the host (its `docker-compose.yaml`: Karavan reads it, and it must be plain `host:container`).

## Schema assurance

*Designer: step 1.1 Decode.*

Four layers, in this order. A record that fails any of them is dead-lettered with the reason shown.

| # | Layer | Catches | Reason |
|---|---|---|---|
| 1 | Wire format: the record starts with the Schema Registry header (zero byte + 4-byte schema id), and the id is in `app.schema.allowedIds` | A record not written through the registry; a producer moving to a new schema version without anyone approving it. An id with the top bit set reads as a negative number and is refused unless it is listed | `no-schema-header`, `header`, `schema-id` |
| 2 | The JSON Schema the registry holds **for that id** | A wrong type, such as `sequence` sent as a string | `schema-violation` |
| 3 | Our own contract (below) | Everything the registered schema does not say: it has no `required` list, so it alone would accept a record with no `conversation_id` | `json`, `field-missing`, `field-malformed`, `field-range`, `payload`, `metadata`, `occurred-at` |
| 4 | The database itself: `CHECK` and `NOT NULL` constraints | The last line of defence | `store-rejected` |

**A new schema version gets a new id.** Its records are refused (dead-lettered) until the id is added to
`app.schema.allowedIds` on purpose. This is the guard against silent drift: the subject's compatibility is `NONE`,
so the registry itself would let anything change.

Schemas are fetched once per id and kept, because ids never change. If the registry cannot be reached, records are
**not** dead-lettered: they wait (see the outcomes table).

### The contract (layer 3)

The rules of the original's decoder, plus `actor`:

| Field | Rule |
|---|---|
| `conversation_id` | required; `^[A-Za-z0-9._:-]{1,128}$` |
| `sequence`, `event_version` | required integers, 1 to 2,147,483,647 |
| `event_name` | required; at most 128 characters |
| `occurred_at` | required; an RFC 3339 time |
| `id_yape_account` | optional; the same pattern as `conversation_id` |
| `flow` | optional; `^[a-z][a-z0-9_]{0,63}$` |
| `actor` | optional; `user`, `agent` or `system` |
| `payload`, `metadata` | required; JSON (a string holding JSON, as Debezium sends JSONB, or an object) that is an object or array |
| unknown fields | ignored, so a new column does not break the consumer |

An empty or `null` value, or a registry header followed by an empty or `null` JSON, is a tombstone and is skipped. A value with anything after the top-level JSON value is refused as `json`, as the original's parser does.

## Projection: what becomes a message

*Designer: step 1.1 Decode.*

| Event | Role | Text | Author |
|---|---|---|---|
| `UserMessageSent` | `appUser` | `payload.text` | the account (`id_yape_account`) |
| `AgentMessageSent` | `appMaker` | `payload.text` | `ms-ml-genai-agent-bs` |
| `GuardrailResponseSent` | `appMaker` | `payload.response_text` | `ms-ml-genai-agent-bs` |

Every other event is stored in the inbox only. A chat event with no text, a non-boolean `truncated`, or (for a
user message) no account is **stored without a message** and counted as invalid, never dropped. An empty text is
valid.

## The store: one statement, one transaction

*Designer: steps 2.1 Write and 2.2 Decide the outcome.*

`store-conversation-history.camel.yaml` (step 2.1) runs a single `WITH` statement, so Postgres applies all or nothing:

1. insert the event into `conversation_inbox`, `ON CONFLICT (conversation_id, sequence) DO NOTHING`;
2. if it was new and projectable, upsert the message (`xmax = 0` tells a new row from an update);
3. if the message was new and has an account, add one to its summary and widen the first and last times;
4. return `inserted`, `projected` and `same`, where `same` says whether the inbox row at that position equals this event.

## What happens to each kind of record

*Designer: steps 1.2 Route, 1.3 Commit, 2.2 Decide the outcome, and the error rules (`errorRules`).*

| Case | What happens | Offset |
|---|---|---|
| New event | Stored; a chat event also gets its message and summary. Log: `Event stored ...` | committed |
| Already stored, identical | Nothing written (a redelivery). Log: `Event duplicate ...` | committed |
| A **different** event at a stream position already taken | Refused: DLQ `store-rejected`. Without this it would pass for a redelivery and be lost without a trace. | committed |
| Tombstone | Skipped. Log: `Tombstone skipped ...` | committed |
| Poison (any reason above) | One `ERROR` line on the `dlq` logger | committed |
| Constraint violation or data the store cannot hold (SQLSTATE class 22 or 23) | DLQ `store-rejected`, field `sqlstate-<code>` | committed |
| **Transient**: database unreachable or timing out, registry unreachable | Retried in place **forever**, the wait doubling from 1 s to 30 s, a `WARN` each time. Never dead-lettered. | not moved until it works |
| **Unexpected**: anything no rule recognizes (a bug) | Tried 3 more times, then DLQ `unexpected` with the exception class in `field=` | committed |

A poison record never blocks a partition: retrying cannot help, so it is logged and skipped. A database outage
blocks it on purpose, and nothing is lost. An outage longer than `max.poll.interval.ms` (5 minutes) makes the broker
drop the consumer from the group; the record in hand is then delivered again and counted as a duplicate.

Why unexpected errors are dead-lettered and not retried forever: retrying a bug cannot succeed, and an endless loop
would hide it behind a stuck partition. A burst of `reason=unexpected` lines is loud and easy to alert on, and
because the line keeps the body, the records can be replayed after the fix.

## The dead-letter log

*Designer: route 3, steps 3.1 to 3.3.*

One line per poison record, on its own logger, `dlq`, at `ERROR`:

```
DLQ reason=field-range field=conversation_id topic=yape.public.conversation_events partition=3 offset=19 key=\x00\x00\x00\x00\x01{"conversation_id":"bad id"} detail=not 1 to 128 of letters, digits and . _ : - body=\x00\x00\x00\x00\x02{"conversation_id": "bad id", "sequence": 1, ...}
```

| Part | Meaning |
|---|---|
| `reason` | Why it was refused (tables above). |
| `field` | The offending field, or for `store-rejected` the SQLSTATE, or for `unexpected` the exception class. |
| `topic` `partition` `offset` | Where the record is, so it can be found and replayed. |
| `key`, `body` | The record as text. The Schema Registry header's zero bytes are shown as `\x00`. Capped at 16,384 characters. |
| `detail` | A short explanation. |

> **The body is logged in full.** It holds conversation text and account IDs. That is acceptable with fake data on a
> local machine, and it makes replay possible. The reference service deliberately logs no content. Before using real
> data or a shared environment, remove `body=${exchangeProperty.rawText}` from the `logDeadLetter` step in
> `handle-errors-and-dead-letters.camel.yaml`.

Read it:

```bash
docker logs conv-history-consumer 2>&1 | grep 'DLQ reason='
# count by reason
docker logs conv-history-consumer 2>&1 | grep -o 'DLQ reason=[a-z-]*' | sort | uniq -c
```

**Replay** after a fix: rewind the group to the offsets in the lines (see the [cheatsheet](cheatsheet.md#consumer-group-offsets)).
A log is not a queue: if you need records kept and re-readable independent of container logs, copy them to a Kafka
topic from `direct:deadLetter`.

### Kafka header names (Camel 4)

Use these in `${header.<name>}`. The old `kafka.TOPIC` style names do not exist in Camel 4 and resolve to empty text.

| Header | Holds |
|---|---|
| `CamelKafkaTopic` / `CamelKafkaPartition` / `CamelKafkaOffset` | Where the record is |
| `CamelKafkaKey` | The key (framed like the value: header + `{"conversation_id": ...}`) |
| `CamelKafkaTimestamp` | Record timestamp, epoch milliseconds |

## `application.properties` changes from the template

| Property | Template | Now | Why |
|---|---|---|---|
| `camel.main.logLanguage` | `groovy` | `simple` | With `groovy`, every `log` message is compiled as a Groovy script, so the normal `${...}` text fails. |
| `camel.management.enabled` / `.port` / `.path` | absent | `true` / `9876` / `/q` | Makes health available at `/q/observe/health`, the URL Docker's healthcheck calls. Without them the container is `unhealthy` forever. |
| `camel.jbang.dependencies` | `camel-console` | adds `camel-sql`, `camel-jackson`, the Postgres driver, HikariCP, `json-schema-validator` | Downloaded from Maven Central when the container starts. |
| `camel.beans.pgDataSource.*` | absent | a Hikari pool, `driverClassName=org.postgresql.Driver` | The driver must be named: DevMode loads dependencies in its own classloader. |

## Run it

1. Start the cluster and Karavan ([setup guide](karavan-setup.md)); apply the `camel_history` migrations.
2. In Karavan open `conv-history-consumer` and click **Run at the top right of the page**. (The Run in the Containers tab
   is a different button and does not start DevMode: see [troubleshooting](troubleshooting.md#which-button-does-what).) The first run pulls the DevMode image
   (about 1.5 GB) and downloads the dependencies, so allow a few minutes.
3. Watch it: Karavan's **Log** tab, or `docker logs -f conv-history-consumer`. A healthy start shows
   `Routes startup (total:3)` (`routeReceive`, `routeStore`, `routeDeadLetter`), then the group joins and gets all 12 partitions.
4. `docker ps --filter name=conv-history-consumer` says `(healthy)` after about 25 seconds.

## Change it

Karavan stores the files in its database, and DevMode copies them into the container when it is created.

1. Change the files in Karavan, or in this repo and then **Push** and **Pull** (see [setup](karavan-setup.md#how-karavan-and-git-work-together)).
2. **Containers** tab → **Stop** → **Delete** (each asks you to confirm), then **Run at the top right of the page**. Do not use
   the Run of the Containers tab for this: it is for built images, and a stopped container keeps its old files.

For quick iterations without Karavan, run the same image against the files in this repo:

```bash
./scripts/devmode-local.sh conv-history-consumer APP_TOPIC=camel.test.events APP_KAFKA_GROUPID=camel-test
docker logs -f devmode-local-conv-history-consumer
./scripts/devmode-local.sh stop conv-history-consumer
```

## Test it

Two scripts, both against the local cluster; `test-consumer.sh` uses a **throwaway topic** and group:

| Script | What it proves | Time |
|---|---|---|
| `./scripts/test-consumer.sh` | 37 checks. Chat, non-chat, duplicate and conflicting events; summary counts; 19 dead-lettered records, each with its exact reason (17 poison, and 2 that the store refuses: a NUL in a text and in a jsonb); a tombstone; no lag (no poison record blocks a partition); an unreachable database is retried, not dead-lettered, and recovers; an unexpected error is dead-lettered after 3 retries. | about 1.5 min |
| `./scripts/test-camel.sh consumer` | 80 JUnit tests: the decoder at the edges of every field (65, against the real Schema Registry), and routes 2 and 3 on the real Postgres (15: stored, duplicate, conflict, NUL, a million characters, 16 threads on one event, 30 on one conversation). About 25 s. | |

`test-consumer.sh` touches only the rows whose conversation id starts with `t-` (it deletes them before and after), and the JUnit tests only rows keyed `jt-`.
Neither writes to the real topic or to the `public` tables.

To publish a fake record to the real topic by hand, remember it needs the Schema Registry header, or it is
dead-lettered as `no-schema-header`, and **Kafka cannot delete records**: if the original service starts later it reads
them too. Always use obviously fake IDs. Two unframed test records from setup (`test-karavan-conv-0001`, `-0002`) are
already on the topic and show up as `no-schema-header` in the DLQ log.

## Replay messages

A consumer group commits its position, so a message that was already read is not read again, even if it was
dead-lettered. To read it again, rewind **this group only**. The group must be inactive: stop the container and wait
about 45 seconds. See the [cheatsheet](cheatsheet.md#consumer-group-offsets), including the dry run you should always
do first.

## Known limits

- **Two writers on the same event.** The duplicate check reads the inbox in the same statement. If a second consumer
  commits the same stream position between the statement's snapshot and its insert, a genuine duplicate could be
  reported as `store-rejected`. It needs two concurrent consumers; with one group member it cannot happen.
- **No rebuild command.** The original can replay its inbox into messages and summaries (`consumer rebuild`). That is
  not ported. It would be a single `INSERT ... SELECT` from `conversation_inbox`.
- **A lone surrogate becomes `?`.** A text with half of an emoji (`\uD800`) cannot be UTF-8; the JDBC driver stores `?` in its place and the stream goes on.
- **Lowercase `t` and `z` in `occurred_at` are accepted** (RFC 3339 allows them; Java parses them). The original's Go parser refuses them. No producer writes them.
- **The DLQ is a log.** It lives as long as the container's logs do.
- **The registry schema is read at the first record,** not at startup. An unreachable registry shows as `WARN` retries
  on the first record, not as a failed start.

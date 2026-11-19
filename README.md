# ms-ml-conv-history-consumer

An Apache Camel Karavan project that reads the conversation events topic and **builds the conversation history in Postgres**.
It is the consumer half of [`ms-ml-camel-conv-history-bs`](../ms-ml-camel-conv-history-bs), on its own: no Microsoft Foundry, no sync,
no API. The Go reference is `ms-ml-conv-history-bs` (its `cmd/consumer`).

```
agent -> event store -> Debezium -> Kafka (yape.event_store.conversation_events) -> this consumer -> conversation_* tables
                                                                                   inbox, messages, summaries
```

## What is in the project

The Karavan project is the folder [`ms-ml-conv-history-consumer/`](ms-ml-conv-history-consumer). Its files are named for the task each one does:

| File | Task | Route |
|---|---|---|
| `consume-conversation-events.camel.yaml` | Start from the Kamelet source, decode and validate the record, hand it on | 1 Receive |
| `store-conversation-history.camel.yaml` | Write the event to the three tables in one statement (**the store route**) | 2 Store |
| `handle-errors-and-dead-letters.camel.yaml` | The error rules, and the dead-letter log for records that can never be stored | 3 Dead-letter |
| `*.java` | The decoder, the store outcome, the poison exception, the store refusal | |
| `application.properties` | Settings (`app.*`), dependencies, datasource | |

The Kafka reading and the offset commit are not in the project: they are in the **Kamelet** [`kamelets/kafka-at-least-once-source.kamelet.yaml`](kamelets/kafka-at-least-once-source.kamelet.yaml), which every consumer can reuse
([how: docs/kamelets.md](docs/kamelets.md)).

Route 2 cannot be reached without route 1 (the only caller) and route 3 (where a record it refuses goes), so the three stay together.

## Run it

```bash
../cluster/scripts/up.sh            # the shared cluster: Kafka, Postgres, Schema Registry
cp .env.example .env                # then set GITHUB_MALMEIDAP_PAT (git-ignored)
./scripts/up.sh                     # Karavan on http://localhost:9997
./scripts/test-all.sh               # docs check, JUnit, and the consumer on a throwaway topic
```

Then in Karavan open `ms-ml-conv-history-consumer` and press the **top-right Run**. Needs the tables of the schema `history`, which come from the
migrations of [`ms-ml-conv-history-migrations`](../ms-ml-conv-history-migrations) (`make migrations/up`).

## Documentation

See [docs/](docs/README.md). **Picking the work up? Start with [docs/handoff.md](docs/handoff.md).**

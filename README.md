# ms-ml-conv-history-consumer

An Apache Camel Karavan project that reads the conversation events topic and **builds the conversation history in Postgres**.
It is the consumer half of [`ms-ml-camel-conv-history-bs`](../ms-ml-camel-conv-history-bs), on its own: no Microsoft Foundry, no sync,
no API. The Go reference is `ms-ml-conv-history-bs` (its `cmd/consumer`).

```
agent -> event store -> Debezium -> Kafka (yape.public.conversation_events) -> this consumer -> camel_history
                                                                                   inbox, messages, summaries
```

## What is in the project

The Karavan project is the folder [`ms-ml-conv-history-consumer/`](ms-ml-conv-history-consumer). Its files are named for the task each one does:

| File | Task | Route |
|---|---|---|
| `consume-conversation-events.camel.yaml` | Read a record from Kafka, decode and validate it, hand it on, commit the offset | 1 Receive |
| `store-conversation-history.camel.yaml` | Write the event to the three tables in one statement (**the store route**) | 2 Store |
| `handle-errors-and-dead-letters.camel.yaml` | The error rules, and the dead-letter log for records that can never be stored | 3 Dead-letter |
| `*.java` | The decoder, the store outcome, the offset commit, the poison exception, the store refusal | |
| `application.properties` | Settings (`app.*`), dependencies, datasource | |

Route 2 cannot be reached without route 1 (the only caller) and route 3 (where a record it refuses goes), so the three stay together.

## Run it

```bash
../cluster/scripts/up.sh            # the shared cluster: Kafka, Postgres, Schema Registry
cp .env.example .env                # then set GITHUB_MALMEIDAP_PAT (git-ignored)
./scripts/up.sh                     # Karavan on http://localhost:9997
./scripts/test-all.sh               # docs check, JUnit, and the consumer on a throwaway topic
```

Then in Karavan open `ms-ml-conv-history-consumer` and press the **top-right Run**. Needs the `camel_history` tables, which come from the
migrations of `ml-genai-agents-bs-testing`.

## Documentation

See [docs/](docs/README.md). **Picking the work up? Start with [docs/handoff.md](docs/handoff.md).**

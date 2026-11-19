# Handoff: where this stands and how to keep going

Read this first if you are picking the work up (a person or another agent). Everything else is linked from [README.md](README.md).

## What this is

One Apache Camel Karavan project, `ms-ml-conv-history-consumer`, that reads the Kafka topic `yape.public.conversation_events` (the Debezium
outbox of the agent's event store) and builds the conversation history in the Postgres schema `camel_history`: `conversation_inbox`,
`conversation_messages` and `conversation_summaries`. It validates each record against the Schema Registry and writes a record it can
never store to a dead-letter log.

**Where it came from.** It is the consumer project of the repository `ms-ml-camel-conv-history-bs`, copied here on its own, **without
Microsoft Foundry**: no sync project, no API project, no token broker. The behaviour is the same as the reference Go consumer
(`ms-ml-conv-history-bs`, `cmd/consumer`). The code was copied, not rewritten; its tests came with it.

**What changed in the copy**

| | Before (`ms-ml-camel-conv-history-bs`) | Now |
|---|---|---|
| YAML files | `route-receive`, `route-store`, `route-error-handling` | named for the task: `consume-conversation-events`, `store-conversation-history`, `handle-errors-and-dead-letters` (route ids and step numbers are unchanged, so logs and the Designer read the same) |
| Consumer group | `ms-ml-camel-conv-history-bs` | `ms-ml-conv-history-consumer`, so it never competes with the consumer of the other repository. A new group starts at the beginning of the topic; that is safe, the inbox refuses a duplicate |
| Compose and `.env` | Karavan, its database, the registry **and** the Foundry token broker | Karavan, its database and the registry |
| Scripts and docs | all three projects, Foundry, the fake Foundry, oracle, parity check, mutation experiment | the consumer's only |
| Kafka reading and commit (branch `kamelets`) | In route 1, with a commit step 1.3 and another in route 3 (3.3) | In the Kamelet `kafka-at-least-once-source`; the project has no commit step. Steps 1.3 and 3.3 no longer exist |

Not copied on purpose: `parity-check.sh` (compares with the Go consumer's tables and empties `camel_history`) and the mutation experiment
(its consumer mutations are in the original repository's `tests/mutation/`).

## Status

| | |
|---|---|
| GitHub | `m-almeida-p/ms-ml-conv-history-consumer`, branch `main`. The consumer starts from the Kamelet `kamelets/kafka-at-least-once-source.kamelet.yaml`, which reads the topic and commits the offset after the route. The branch `kamelets` is the same code, kept for reference |
| Backup | The tag **`backup-before-kamelets`** is the previous version, where the consumer reads Kafka itself with a commit step in routes 1 and 3 |
| Tests | Same behaviour as the backup: all 37 system checks and the 80 JUnit tests pass ([testing.md](testing.md#last-run)) |
| Run under Karavan | **Verified 2026-11-19.** Karavan copied the Kamelet into the container by itself, the routes started (`total:3 kamelets:1`), the consumer joined its own group and replayed the topic: 119 duplicates, none stored, lag 0, the tables unchanged, the 2 known unframed test records dead-lettered. It runs on 8087 next to the other stack's consumer on 8081 |
| Karavan | `consumer-karavan` on `http://localhost:9997`, reading this repository (branch `main`) with the same token as the other stack |

**To go back to the plain routes:** `git revert` the Kamelet commits, or `git reset --hard backup-before-kamelets` and force-push (ask the owner first), then Pull in Karavan.

## Is it healthy? (one minute)

```bash
docker ps --format '{{.Names}} {{.Status}}' | grep ms-ml-conv-history-consumer     # healthy
docker exec yape-kafka kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group ms-ml-conv-history-consumer \
  | awk '$6 ~ /^[0-9]+$/ {l+=$6} END {print "lag", l+0}'                      # 0
docker logs ms-ml-conv-history-consumer 2>&1 | grep -c 'DLQ reason='               # dead letters so far; see the reasons
```

Needs the shared cluster up (`../cluster/scripts/up.sh`, network `yape-local`) and Karavan (`./scripts/up.sh`, UI on
`http://localhost:9997`, bound to `127.0.0.1`). Start-up from nothing: [karavan-setup.md](karavan-setup.md).

**To add another consumer** that reads a Kafka topic, read [kamelets.md](kamelets.md): the Kamelet is configured, not rewritten.

## The rules the owner set

These stand until the owner changes them.

- **Never run the Go service** (`ms-ml-conv-history-bs`) or its consumer.
- **Never commit in `../ml-genai-agents-bs-testing`.** It is the producer repo and holds the owner's uncommitted migrations. A **new table
  goes through a migration there**, not through SQL here. Never run that repo's `run.sh --clean`.
- **Never reset or delete the consumer group `ms-ml-conv-history-bs`** (the original service's). Resetting `ms-ml-conv-history-consumer`
  is allowed only on purpose, with the project stopped.
- **Secrets only in `.env`** (git-ignored). Never in chat, in a file, in a log or in a command line you paste. Never run `bash -x` on a
  script that reads `.env`. **Run `./scripts/scan-secrets.sh` before every push**; it prints only counts.
- **Fake data only.** Kafka cannot delete records: use obviously fake ids if you publish by hand.
- **The owner does the Karavan login.** Do not try to log in for them.
- Commits are free (one per task). Pushing is for the owner to authorise here: this repository has no remote yet.

## Changing something safely

1. **Test first.** For a bug, write the failing test, see it fail, then fix ([testing.md](testing.md#adding-a-test)).
2. `./scripts/test-camel.sh` while you work, then `./scripts/test-all.sh` before you finish.
3. `python3 -I scripts/check-docs.py` if you touched a route, a step label or a doc: every `Docs:` pointer in a Designer note must
   resolve, every link work, every label fit the Designer (32 characters), every diagram render.
4. Commit, `./scripts/scan-secrets.sh`, push.
5. **Deploy to Karavan:** open the project, **Source**, **Pull**, confirm; `docker rm -f ms-ml-conv-history-consumer`; reload the page; press
   the **top-right Run** (not the one in the Containers tab); wait for `healthy`. A changed `application.properties` or `docker-compose.yaml`
   needs the container recreated, not restarted.

Faster loop without Karavan: `./scripts/devmode-local.sh ms-ml-conv-history-consumer [NAME=value]` runs the project in the same DevMode image
on a throwaway topic. The tests use it.

Things that cost time before ([troubleshooting.md](troubleshooting.md) has each with its fix): a stray file in a project folder crashes its
container (Run copies every file); Karavan's compose parser reads ports as `host:container` and fails on `127.0.0.1:8087:8080`; macOS
`/private/tmp` is invisible to Docker (use `$HOME` or `.devmode-local/`); `docker exec -i` swallows a script's stdin; BSD `sed -i` needs a
suffix; a Camel `onException` inherits the Dead Letter Channel's exponential back-off unless you switch it off.

## Scripts that can hurt

| Script | Why |
|---|---|
| `scripts/clean.sh` | Removes Karavan's containers **and its database volume** (the project is safe: it is in Git) |
| `docker rm -f ms-ml-conv-history-consumer` | Normal when redeploying, but the project is down until you press Run again |

## Not ready for production

It is a local PoC and says so. Known, accepted by the owner:

- **Chat text and the account id are written to the dead-letter log** (and so to container logs). The offset is enough to replay; log only
  reason, field, topic, partition and offset if this goes further.
- **The port 8087 is on all interfaces** (Karavan's compose parser cannot bind it to `127.0.0.1`). Use a host firewall if the network is shared.
- The Camel management port (9876) has `sendEnabled`, `downloadEnabled` and the dev console on. It answers only health today; turn them off
  before sharing the network.
- The service uses the `postgres` superuser with no password (the cluster's convention).

## Open items

| # | What | Notes |
|---|---|---|
| 1 | Rule for Kafka commit failures | `CommitFailedException` and `RebalanceInProgressException` are in no `onException` rule, so one would be dead-lettered as `unexpected`. Add a rule that ignores them (the record redelivers); not done because it could not be provoked to test |
| 2 | Dependency audit | `postgresql` is declared at 42.7.12; at run time Camel's downloader also puts 42.7.13 on the classpath (which one loads was not determined). HikariCP 6.2.1 and json-schema-validator 1.5.6 have newer **major** versions that were not taken. No scan tool was run |
| 3 | The owner's migrations `000001`-`000004` | Uncommitted in the producer repo. The tables here depend on them |
| 4 | The old consumer is still in `ms-ml-camel-conv-history-bs` | Both run at once, on purpose: different ports, names and groups ([table](karavan-setup.md#running-next-to-ms-ml-camel-conv-history-bs)). They write the same three tables, which is safe because the inbox refuses a duplicate, but they do the same work twice |

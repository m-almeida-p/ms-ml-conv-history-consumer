# Testing

Every test of this repository, what it proves, what it needs and how to run it. `./scripts/test-all.sh` runs all of them and ends with
one table.

| Suite | What it needs |
|---|---|
| `check-docs.py` | Nothing (Mermaid CLI for the diagrams, if installed) |
| JUnit, `test-camel.sh` | Docker, the cluster (Postgres, Schema Registry) |
| `test-consumer.sh` | Docker, the cluster (Kafka, Postgres, Schema Registry) |

```bash
./scripts/test-all.sh                 # the three suites
./scripts/test-all.sh --only camel --stop
```

The full output of each suite is kept in `.test-logs/<suite>.log` (git-ignored). A suite that fails prints its first failing lines.

## What is safe to run

Every suite works on **its own rows and topics**: fixtures are keyed with a prefix (`t-` for `test-consumer.sh`, `jt-` for the JUnit
tests) and removed at the end, and each throwaway topic and consumer group is deleted. The real history in the conversation tables and the real
consumer groups are never touched. If you add a script, scope every `DELETE` to your own prefix and never `TRUNCATE`.

## JUnit (`tests/camel/consumer`)

One Maven module, run in a Maven container on the cluster network (`maven:3.9-eclipse-temurin-21`, cache in the Docker volume
`camel-tests-m2`, so only the first run downloads). It compiles **the project's own Java files** (`sourceDirectory` points at
`ms-ml-conv-history-consumer/`), so a test breaks when the code does, and the YAML routes are loaded from the project folder. Nothing is a copy.

```bash
./scripts/test-camel.sh                         # the whole module
./scripts/test-camel.sh consumer DecoderTest    # one class (a comma list works)
```

| Class | What it proves |
|---|---|
| `DecoderTest` | The decoder at the edge of every field, against the real Schema Registry (read only); tombstones |
| `DeadLetterSinkTest` | The shared `dead-letter-sink` Kamelet on its own, on the cluster's Postgres: the bytes as they came, a text key as UTF-8 or no key, the Kafka headers as JSON (base64 for what is not text, Camel's own and a route's left out), NULs removed from the texts and the detail cut, the attempt (1, or the retries plus 1 for an unexpected error), the exception class only for an unexpected error, nulls where the exchange has nothing, the readable key for the log |
| `StoreRouteTest` | Routes 2 and 3 with their real error rules on the cluster's Postgres: stored, duplicate, conflict, a NUL (refused for good), a million characters, 16 threads on one event, 30 on one conversation; and the dead-letter table: a refused record stored byte for byte with its coordinates, the same record twice leaving one row, a record the store refuses keeping the original bytes and not the decoded event, an unexpected error with its class and attempts |

**What the JUnit tests do not cover:** route 1 and the Kamelet (the Kafka subscription and the offset commit). `test-consumer.sh` does: it runs the project with its Kamelet, as Karavan does. The route tests assert that route 2 does **not** commit (the Kamelet does) and that route 3 commits exactly once, and only after its dead-letter row exists (a handled error stops the route that called route 1, so the Kamelet's commit does not run for a dead letter).

## `test-consumer.sh`

Starts the project with `devmode-local.sh` on a throwaway topic and group, publishes about 30 records in the format Debezium writes (Schema
Registry header and JSON, schema id 2), waits until all are handled, and checks: the three tables (rows, roles, authors, summary counts),
the dead-letter table (one row per poison record with its exact reason, the record byte for byte, its coordinates and Kafka's timestamp) and the log (one line each, without the record), that every offset was committed (no lag, so no poison record
blocks a partition), that an unreachable database is retried and recovers (a refused record that meets the outage waits, and is written once the database is back), and that an unexpected error is dead-lettered after three
retries with its class and four attempts, even when the store's schema is missing. It touches only rows whose conversation id starts with `t-`.

## Adding a test

1. A bug: write the test that **fails first**, see it fail on the old code, then fix.
2. Logic with no I/O: a JUnit test. Anything with a database: the same, using the cluster's Postgres (rows keyed `jt-`).
3. A behaviour that needs Kafka or a killed process: a section of `test-consumer.sh`, on a throwaway topic and group, scoped to your prefix.
4. Run `./scripts/test-all.sh`, and `python3 -I scripts/check-docs.py` if you touched a route, a label or a doc.

## Last run

2026-11-19, route 3 became the shared Kamelet `dead-letter-sink`: **all 3 suites green**: docs check, **90 JUnit tests** (the row's unit tests and the decoder's byte tests moved to `DeadLetterSinkTest`, 8 tests on
the Kamelet; `DecoderTest` 63, `StoreRouteTest` 19) and **46 system checks** with both Kamelets in the loop (the outage, the trailing refused record and the unexpected error included). No test row left.

2026-11-19, with the dead letters in `dead_letter.dead_letters`: **all 3 suites green**: docs check, **95 JUnit tests** and **46 system checks**, no test row
left in any table.

**A defect the checks could not see, found by running it under Karavan:** a dead-lettered record's offset was never committed (a handled error stops the route that called route 1,
so the Kamelet's commit does not run). Every lag check of `test-consumer.sh` missed it, for two reasons: records that were stored followed the dead letters and their later commit
covered the earlier offsets, and the `awk` kept only rows whose lag was a number, while a partition that was never committed shows `-`. Now: route 3 commits (step 3.5); the lag
is the log end minus the committed offset (a never-committed partition counts); the outage phase ends with a refused record, alone at the end of its partition. Without step 3.5 the
checks fail (15, 1 and 2 offsets uncommitted); with it they pass. The two things worth knowing: a refused record that meets a database outage is retried in place and written exactly once when the database returns, with
its offset committed; and the redelivery counter was **not** usable when the Dead Letter Channel calls route 3: a record Camel had tried 4 times was recorded as attempt 1. So the
attempt of an unexpected error is `app.unexpected.maxRetries` + 1, from the setting the retry policy reads, and the system test checks both the 4 failed deliveries and `attempt = 4`.

Earlier runs:

2026-11-19, on the plain-routes version (tag `backup-before-kamelets`): **all 3 suites green**. `check-docs.py`: 16 pointers, 46 links, 26 labels, 5 diagrams. JUnit: **80 tests**
(`DecoderTest` 65, `StoreRouteTest` 15). `test-consumer.sh`: all checks passed (about 2 minutes). The consumer's copy of the test that kept the sync's
and the consumer's decoder identical was removed, because the sync is not in this repository.

Rerun after the project folder was renamed to `ms-ml-conv-history-consumer` and the ports and names were made unique: still all green.
The new Karavan stack was started next to the other one (UI 9997 and 9998 both answer, every container name resolves to one container).

Branch `kamelets` (the consumer starts from the Kamelet `kafka-at-least-once-source`), 2026-11-19: **all 3 suites green again**: docs check, 80 JUnit tests,
and all 37 system checks of `test-consumer.sh`, including the commit behaviour (an outage leaves the offset uncommitted, recovery commits it, poison and
unexpected records are committed). Route 3 had lost its commit step in this change, and the checks could not see it (see the 2026-11-19 entry above): it has one again.

**Under Karavan (Run in the UI), branch `kamelets`:** verified. The Karavan Designer draws the Kamelet as a typed source (Topic, Brokers, Consumer group, Where to start); Run
copies `kafka-at-least-once-source.kamelet.yaml` into the container; the consumer joins its group, reads the 119 existing events as duplicates, ends with lag 0 and leaves
the tables as they were. Getting there needed the patched DevMode image ([why](karavan-setup.md#running-next-to-ms-ml-camel-conv-history-bs)).

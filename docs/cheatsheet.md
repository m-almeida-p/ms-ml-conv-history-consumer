# Cheatsheet

Commands that were useful. Container names: `karavan`, `karavan-db`,
`karavan-registry`, `conv-history-consumer` (Karavan stack and consumer) and
`yape-kafka` (the shared cluster's broker).

## Karavan stack

```bash
./scripts/up.sh                 # start (needs ../cluster up first)
./scripts/stop.sh               # stop, keep data
./scripts/start.sh              # start again
./scripts/logs.sh -f            # follow logs
./scripts/clean.sh              # remove containers AND the Postgres volume (projects stay in Git)

docker logs karavan --tail 80   # the place to look when the UI shows only a vague error
docker compose config -q && echo OK    # validate docker-compose.yml
docker compose up -d karavan    # apply a compose change; recreates only karavan
```

Karavan is ready at <http://localhost:9998> once `docker logs karavan` shows
`started in ...s` and `Projects loaded`. Readiness probe:
`curl -s -o /dev/null -w '%{http_code}\n' http://localhost:9998/public/readiness` → `200`.

## The consumer container

```bash
docker ps -a --filter name=conv-history-consumer --format '{{.Names}}\t{{.Status}}'
docker logs -f conv-history-consumer

# Only the received-message lines, without ANSI colours
docker logs conv-history-consumer 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep 'Received kafka message'

# Health: Docker's own verdict, and the endpoint it calls
docker inspect conv-history-consumer --format '{{.State.Health.Status}}'
docker exec conv-history-consumer curl -s -o /dev/null -w '%{http_code}\n' http://localhost:9876/q/observe/health

# What Camel serves on the app port
docker exec conv-history-consumer curl -s http://localhost:8080/observe/health
```

Applying a change = Karavan **Containers** tab → **Stop** → **Delete** (each asks you to confirm), then **Run at the top right of the
page** (not the Run of the Containers tab; see [which button does what](troubleshooting.md#which-button-does-what)).

### Dead-letter log and results

```bash
docker logs conv-history-consumer 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep 'DLQ reason='      # every poison record
docker logs conv-history-consumer 2>&1 | grep -o 'DLQ reason=[a-z-]*' | sort | uniq -c         # count by reason
docker logs conv-history-consumer 2>&1 | grep -E 'Event (stored|duplicate)' | tail             # what was stored

# What the consumer wrote (schema camel_history), and the original consumer's result next to it
docker exec yape-postgres psql -U postgres -d postgres -c "
  SELECT 'camel_history' AS who, (SELECT count(*) FROM camel_history.conversation_inbox) AS inbox,
         (SELECT count(*) FROM camel_history.conversation_messages) AS messages,
         (SELECT count(*) FROM camel_history.conversation_summaries) AS summaries
  UNION ALL
  SELECT 'public (original)', (SELECT count(*) FROM public.conversation_inbox),
         (SELECT count(*) FROM public.conversation_messages), (SELECT count(*) FROM public.conversation_summaries)"
```

### Local runner and tests (no Karavan)

```bash
./scripts/devmode-local.sh conv-history-consumer APP_TOPIC=camel.test.events APP_KAFKA_GROUPID=camel-test   # run the project
./scripts/devmode-local.sh stop conv-history-consumer
./scripts/test-consumer.sh        # 37 checks on a throwaway topic (about 2 min); only touches rows whose id starts with t-
```

## Kafka (inside the `yape-kafka` container)

From the host use `localhost:9092`. Inside Docker use `kafka:29092`.
The commands below run in the broker container, so either works.

```bash
# Topics
docker exec yape-kafka kafka-topics --bootstrap-server localhost:9092 --list

# Earliest and latest offset per partition (equal numbers = nothing readable)
docker exec yape-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic yape.public.conversation_events --time -2   # earliest
docker exec yape-kafka kafka-run-class kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic yape.public.conversation_events --time -1   # latest

# Who reads the topic
docker exec yape-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --list
docker exec yape-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group ms-ml-conv-history-consumer
docker exec yape-kafka kafka-consumer-groups --bootstrap-server localhost:9092 --describe --group ms-ml-conv-history-consumer --state
```

`--describe` columns: `CURRENT-OFFSET` is where the group is, `LOG-END-OFFSET`
is the newest record, `LAG` is the difference.

The cluster also runs Kafka UI at <http://localhost:8090> for browsing topics.

### Publish a fake test record

See [conv-history-consumer.md](conv-history-consumer.md#test-it) for the full
command. **Records cannot be deleted from Kafka**, so use obviously fake data.

### Consumer group offsets

Replay messages by rewinding **only your own group**
(`ms-ml-conv-history-consumer`). Never reset `ms-ml-conv-history-bs`; that is
the real service's group.

1. Stop the consumer (Karavan **Containers** → **Stop**).
2. Wait until the group is inactive, about 45 seconds:
   ```bash
   docker exec yape-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
     --describe --group ms-ml-conv-history-consumer --state      # wait for "Empty"
   ```
3. **Dry run first.** It changes nothing and prints the new offsets. Compare
   them with the current ones from `--describe` and make sure only the
   partitions you expect move:
   ```bash
   docker exec yape-kafka kafka-consumer-groups --bootstrap-server localhost:9092 \
     --group ms-ml-conv-history-consumer --topic yape.public.conversation_events \
     --reset-offsets --by-duration PT60M --dry-run
   ```
   `--by-duration PT60M` means "the first record newer than 60 minutes ago".
   Partitions with nothing newer stay where they are.
4. Apply: same command with `--execute` instead of `--dry-run`.
5. Run the consumer again.

Other targets: `--to-earliest`, `--to-latest`, `--to-offset N` with a single
`--topic name:partition`, `--shift-by -1`.

## Git

```bash
git fetch origin && git log origin/main --oneline --stat | head    # what Karavan pushed
git pull --ff-only                                                 # bring it into the local clone
```

Karavan push: project **Source** tab → tick files → **Push** → message. Press
**Pull** in Karavan after you push from the command line.

## Reading a Camel jar (useful for header names and property names)

Copy the jar out of the container into a scratch folder and unzip there, not
in the project:

```bash
docker cp conv-history-consumer:/karavan/.m2/repository/org/apache/camel/camel-kafka/4.22.2/camel-kafka-4.22.2.jar /tmp/ck.jar
unzip -p /tmp/ck.jar org/apache/camel/component/kafka/KafkaConstants.class \
  | LC_ALL=C tr -c '[:print:]' '\n' | LC_ALL=C grep -E '^CamelKafka' | sort -u
```

Camel's configuration property catalogue is
`META-INF/camel-main-configuration-metadata.json` inside the `camel-main` jar.

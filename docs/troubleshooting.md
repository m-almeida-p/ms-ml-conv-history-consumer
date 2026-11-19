# Troubleshooting

Every entry here is a problem that actually happened while setting this up.
Format: symptom → cause → fix.

Quick triage:

```bash
docker ps -a --format '{{.Names}}\t{{.Status}}'      # what is running, and is it healthy
docker logs consumer-karavan --tail 80                        # Karavan server errors (the UI hides most causes)
docker logs ms-ml-conv-history-consumer --tail 80          # the consumer's own log
```

When the UI shows only a vague popup, **read `docker logs consumer-karavan`**. The real
reason is almost always there.

---

## Starting Karavan

### `network yape-local declared as external, but could not be found`
**Cause:** the shared cluster is not running, so its network does not exist.
**Fix:** `../cluster/scripts/up.sh`, then `./scripts/up.sh`.

### Karavan starts but cannot reach Git
**Cause:** `GITHUB_MALMEIDAP_PAT` is empty or `.env` is missing. `up.sh` warns
about this.
**Fix:** `cp .env.example .env`, set the token (fine-grained PAT for this repo,
*Contents: read and write*), then `docker compose up -d consumer-karavan`.

### Port already in use / container name conflict
**Cause:** 9997 (UI), 5556 (registry) or 8087 (the consumer) is taken, or another stack uses the
names `consumer-karavan`, `consumer-karavan-db`, `consumer-registry`. The other repository's stack uses 9998, 5555, 8081 and
`karavan*` names on purpose, so the two can run together ([table](karavan-setup.md#running-next-to-ms-ml-camel-conv-history-bs)).
**Fix:** change `KARAVAN_PORT` / `KARAVAN_REGISTRY_PORT` in `.env`. For name
clashes, find who holds it: `docker ps --format '{{.Names}} {{.Ports}}'`.

---

## Running a project (DevMode)

### "Error Starting DevMode container" / `pull access denied`
Log (`docker logs consumer-karavan`):
```
Status 404: pull access denied for apache/camel-karavan-devmode,
repository does not exist or may require 'docker login'
```
**Cause:** Karavan's default DevMode image is `apache/camel-karavan-devmode`
on Docker Hub, which does not exist. The image is published on GitHub's
registry, next to the Karavan image.
**Fix:** `docker-compose.yml`, service `karavan`:
```yaml
KARAVAN_DEVMODE_IMAGE: ghcr.io/apache/camel-karavan-devmode:4.22.2
```
Keep the tag equal to the Karavan version. Recreate Karavan:
`docker compose up -d consumer-karavan`. Projects and the UI session survive because
state is in the Postgres volume.
Verify an image exists before using it: `docker manifest inspect ghcr.io/apache/camel-karavan-devmode:<tag>`.

### A project's container exits right after it starts: `Failed to start route: route1 (source: openapi-rest-camel.yaml:1)`
**Cause:** a file that exists only in Karavan. The **OpenAPI** button of a project's toolbar creates `openapi-rest-camel.yaml`
(pressed by accident, it is easy to miss). Run copies **every file of the project** into the container, and Camel loads a `.yaml` as
routes, so it tried to turn that file into a REST contract and failed. The container exits with code 1.
**Fix:** in the project's **Source** tab, delete the file (the dialog names it; it is not in Git, so there is nothing to push). Remove
the exited container (`docker rm -f <name>`, or **Containers** → Delete), reload the project page, and press the top-right **Run**.
**Prevent:** before Run, look at the Source tab: a file marked `NEW` that you did not write is the suspect. After a Pull, only files
that are in Git should be there.

### Run does nothing after I changed the project's `docker-compose.yaml` port to `127.0.0.1:8087:8080`
**Cause:** Karavan reads the project's compose file when it runs the project, and its parser reads each port as `host:container` numbers.
Karavan's log has `For input string: "127.0.0.1"` (from `DevModeResource`), and no container is created.
**Fix:** write the port as `"8087:8080"`. The consumer cannot be bound to the loopback from the compose file; use the host's firewall
if the port must not be reachable from the network.

### I clicked Stop, Delete or Run and nothing happened
**Cause:** the buttons of the **Containers** tab (Stop, Delete, Run) each open a confirmation dialog, "Confirm stop container?", "Confirm delete container?" or "Confirm run container?", and do nothing until you click **Confirm**. The dialog is easy to miss; the container is unchanged in `docker ps` until you confirm. The square Stop and the top-right Run of the page header do not ask.
**Fix:** look for the dialog and click **Confirm**.

### I clicked Run, a "Confirm run container?" dialog appeared, I confirmed, and nothing started
**Cause:** the page has **two different Run buttons**, and the one in the **Containers** tab is not the one that starts a
project. It runs an *image you built* (Build/Deploy) and asks for confirmation, and with no built image it does nothing:
the Images list is empty and `docker logs consumer-karavan` shows no `DevMode starting` line. The button that starts the project in
DevMode is **Run at the top right of the page**, next to the tab bar.
**Fix:** click the top-right **Run** (it does not ask for confirmation). Karavan then logs
`DevMode starting for <project>`, and `docker ps` shows the container within seconds.

### The top-right Run is not there, only Stop and a trash icon
**Cause:** while a DevMode container exists (running or stopped) the header shows Stop and a trash icon instead of Run.
The Run in the **Containers** tab is then enabled, and is expected to restart that same container with the files it already
has (we always deleted the container instead, so this was not tested).
**Fix:** to start from the latest files, remove the container first: **Containers** tab → **Stop** → **Delete**
(confirm). The top-right **Run** comes back. Click it.

### My change is saved but the running container behaves the old way
**Cause:** DevMode copies the project files into the container when it is created. A running or stopped container keeps
that copy, so it does not see later changes.
**Fix:** **Containers** tab → **Stop** → **Delete** → then the top-right **Run**. The log of the new container starts with
`Extracted file: ...` lines, one per project file: check that they are the files you expect.

### Run seems to do nothing for a few minutes
**Cause:** the first run pulls the DevMode image (about 1.5 GB) and then downloads the dependencies from Maven Central. The
UI shows no progress for either.
**Fix:** wait, and watch Docker instead: `docker images | grep devmode` while it pulls, then
`docker logs -f ms-ml-conv-history-consumer` while it downloads. Later runs reuse both and start in about 20 seconds.

### A popup flashed ("Error Starting DevMode container", ...) and was gone before I could read it
**Cause:** Karavan's error toasts disappear after a few seconds and give only a one-line message.
**Fix:** read the cause in Karavan's server log: `docker logs consumer-karavan --since 5m | grep -E 'DevMode|ERROR'`.

### Header shows a spinner and the container list is empty
**Cause:** the UI polls Karavan every few seconds and redraws, so what it shows can lag behind Docker by a moment.
**Fix:** reload the page. The truth is `docker ps -a`.

---

## The consumer

### Message received, then `MultipleCompilationErrorsException ... LHS of an assignment should be a variable`
**Cause:** `application.properties` has `camel.main.logLanguage=groovy`
(Karavan's template default). Every `log` message is then a Groovy script, and
`Received ... topic=${header...}` is not valid Groovy.
**Fix:** set `camel.main.logLanguage=simple`. The alternative is to write every
message as a Groovy expression, which is harder to read. Recreate the
container.

### Log line prints but values are empty: `topic= partition= offset= key=`
**Cause:** wrong header names. Camel 4 uses `CamelKafkaTopic`,
`CamelKafkaPartition`, `CamelKafkaOffset`, `CamelKafkaKey`,
`CamelKafkaTimestamp`. The old `kafka.TOPIC` forms resolve to empty text and do
not raise an error.
**Fix:** use `${header.CamelKafkaTopic}` etc. To discover names for any
component, read its `*Constants` class in the jar, or log `${headers}` once.

### Container shows `(unhealthy)` but the consumer works
Healthcheck: `curl -f http://localhost:9876/q/observe/health`.
**Cause:** Camel's management server is off by default, so nothing listens on
9876. Health was served only on port 8080 at `/observe/health`.
**Fix:** in `application.properties`:
```
camel.management.enabled=true
camel.management.port=9876
camel.management.path=/q
```
Recreate the container; it reports `(healthy)` after about 25 seconds.
Confirm: `docker exec ms-ml-conv-history-consumer curl -s -o /dev/null -w '%{http_code}\n' http://localhost:9876/q/observe/health` → `200`.
The names come from Camel's `camel-main-configuration-metadata.json`. To check
a property, read that file from the `camel-main` jar.

### I fixed the problem but the message is not processed again
**Cause:** the group committed the offset even though the log step failed, so
the message counts as read.
**Fix:** rewind the group (see the [cheatsheet](cheatsheet.md#consumer-group-offsets)),
or publish a new test record.

### `Assignments can only be reset if the group ... is inactive`
**Cause:** Kafka still counts the stopped container as a member until its
session timeout (45 s) expires. `--describe --state` shows `Stable` until then.
**Fix:** stop the container and wait until the state is `Empty`, then retry.

### Consumer joined the group but no partitions get records
**Cause:** the topic holds nothing newer than the committed offsets. Earliest
and latest offsets can even be equal (retention removed old records).
**Fix:** publish a test record. Check with
`kafka-run-class kafka.tools.GetOffsetShell ... --time -2` (earliest) and
`--time -1` (latest).

### `Failed delivery ... On delivery attempt: N caught: ...` repeating
**Cause:** a record failed and the consumer is retrying it. Read the exception after `caught:`.
- `CannotGetJdbcConnectionException`, `Connection refused`, `schema id N is not available from the schema registry`:
  a **transient** failure. The consumer retries forever, with a growing wait, and moves on when the database or registry is
  back. Nothing is lost and nothing is dead-lettered. Fix the dependency.
- Anything else (`BadSqlGrammarException`, `NullPointerException`, ...): an **unexpected** error. It is tried 3 more times,
  then dead-lettered with `reason=unexpected` and the exception class in `field=`. A burst of those lines is a bug or a
  wrong setting (for example a wrong `app.db.url`, which gives `relation ... does not exist`).

### `DLQ reason=unexpected` for every record
**Cause:** a bug or a setting error that hits all records, such as a missing table or a wrong schema in `app.db.url`.
**Fix:** correct it, restart the container, then replay the records: their offsets are in the lines, and the group can be
rewound (see the [cheatsheet](cheatsheet.md#consumer-group-offsets)).

### `Failed to get driver instance for jdbcUrl=...`
**Cause:** DevMode loads the Postgres driver in its own classloader, where `java.sql.DriverManager` cannot discover it.
**Fix:** name it: `camel.beans.pgDataSource.driverClassName=org.postgresql.Driver`.

### `ClassNotFoundException: ConversationEventDecoder` at startup
**Cause:** `camel.beans.<name>=#class:...` is applied before the project's Java files are compiled, so a class from
those files cannot be found there.
**Fix:** register the class from inside its file with `@BindToRegistry("name")`, and read settings from the Camel
properties at first use, not from `camel.beans.*`.

### `ClassNotFoundException: PoisonRecordException` in an `onException`
**Cause:** an `onException` resolves the exception class by name before the Java files are compiled.
**Fix:** make the exception extend a built-in class and match that one: `PoisonRecordException extends
org.apache.camel.ValidationException`, and the route lists `org.apache.camel.ValidationException`. `ValidationException`
is a checked exception, so methods that throw it declare `throws PoisonRecordException`.

### Poison records are retried 3 times before they reach the dead-letter log
**Cause:** with a Dead Letter Channel as the route configuration's error handler, an `onException` rule **inherits the
handler's redelivery policy** (3 retries) unless it sets its own. A poison record can never succeed, so the retries only
add delay and `Failed delivery ... attempt 0` warnings. The test suite caught this.
**Fix:** give the rules for poison and refused records their own policy: `redeliveryPolicy: {maximumRedeliveries: "0"}`.

### `Unsupported field: description` on a `deadLetterChannel`
**Cause:** the error handler definitions (`deadLetterChannel`, `redeliveryPolicy`) take an `id` but no `description` or
`note`. Routes, steps, `from` and `onException` do.
**Fix:** put the explanation in a YAML comment above the handler.

### `Node should have only have 1 key, was: 4 keys: [id,exception,handled,steps]`
**Cause:** in a `routeConfiguration`, each entry of `onException` must be wrapped as its own `- onException:` node.
**Fix:** `onException:` → `- onException:` → `id`, `exception`, `handled`, `steps` indented under it.

### `YAML DSL compact notation detected`
**Cause:** a warning only. A shorthand such as `simple: "..."` or `constant: "true"` was used.
**Fix:** write it canonically (`simple: {expression: "..."}`), which Karavan's Designer also prefers.

### The log says `Fetching project null files from Platform ... ConnectException`
**Cause:** printed by `scripts/devmode-local.sh`. The DevMode image tries to download the project from Karavan first, and
without a Karavan project id it falls back to the folder it was given.
**Fix:** none needed.

### The local runner starts with `Routes startup (total:0)`
**Cause:** the project folder was not visible in the container. A folder under the system temp directory is not shared
with the Docker VM.
**Fix:** `scripts/devmode-local.sh` copies the project to `.devmode-local/` inside the repo (git-ignored), which is
shared. If you change the script, keep the copy inside the repo.

### A test script waits forever, or says the consumer did not join the group
**Cause:** `scripts/common.sh` sets `pipefail`, and `docker logs | grep -q` then fails with a SIGPIPE status even when it
matches.
**Fix:** read the output into a variable and match it there (`grep -q ... <<< "$out"`), as `log_has` does in the test
scripts.

### `Error getting Pod Events: namespace cannot be null` in Karavan's log
**Cause:** Karavan's Kubernetes service runs a check that makes no sense when
running on Docker.
**Fix:** none needed. It is noise.

### `Registry request failed https://registry:5000/v2/: Failed to create SSL connection`
**Cause:** Karavan queries the local registry over HTTPS, but `registry:2`
serves plain HTTP.
**Impact:** only the *Images* list and *Build/Deploy*. **Run (DevMode) is
unaffected.**
**Fix:** none applied. A proper fix needs a TLS-enabled registry.

---

## Using the Karavan UI

Most of the time lost while building this went to the UI, not to Camel. This section is the map and the traps.

### Which button does what

| Where | Button | What it really does |
|---|---|---|
| Top right of the page | **Run** | Starts the project in **DevMode**: creates the container from the project's files. Shown only while no DevMode container exists. |
| Top right of the page | **Stop** (square) and **trash** | Stop, and delete, the running DevMode container. |
| Top right of the page | **Reload** | Appears while the project runs. We did not use or test it: apply changes with Stop, Delete and Run. |
| **Containers** tab | **Run** | Asks "Confirm run container?" and runs an *image you built*; with a stopped container present it is expected to restart that container as it is (not tested). **Not** the way to start DevMode. |
| **Containers** tab | **Stop** / **Delete** | Stop the container / remove it. **Each opens a confirmation dialog** ("Confirm stop container?", "Confirm delete container?") and does nothing until you click **Confirm**. Deleting removes only the container, never the project. |
| **Containers** tab | **Deploy** | Builds and deploys an image. Not used here. |
| **Source** tab | **Pull** | Replaces the project's files with Git's. Warns that it rewrites everything not committed. |
| **Source** tab | **Push** | Commits and pushes the ticked files. Disabled until a file is ticked. |
| **Source** tab | **Create** / **Upload** | Add a file by typing / from disk, one file at a time. Neither replaces an existing file. |
| **Source** tab | row icons | Edit, delete (asks to confirm), download. Not available for `application.properties` and `docker-compose.yaml`. |
| **Topology** tab | **Properties** | Opens `application.properties` in the code editor. |

### Typing into the code editor scrambles my text
**Cause:** the editor re-parses and reformats the file on every keystroke and
moves the cursor, and its autocomplete and auto-closing brackets interfere.
Long single-line text comes out cut up and repeated.
**Fix:** **paste** instead of typing. Put the whole file on the clipboard, click
in the editor, `Cmd+A`, `Cmd+V`. Short edits to a plain value (such as one
property) type fine.
Tip: Karavan rewrites long strings as folded blocks (`message: >-`). That is
equivalent YAML.

### **Upload** does not replace a file that already exists
**Cause:** Karavan's Upload dialog adds new files and silently ignores one whose name is taken.
**Fix:** delete the file in the Source tab, then upload it again. `application.properties` and `docker-compose.yaml`
cannot be deleted in the UI: change `application.properties` in its editor (paste the whole file), or push from Git and
press **Pull**.

### My edit in the Designer form did not change the file
**Cause:** a value set programmatically did not trigger the form's change
handler.
**Fix:** edit in the **Code** tab and paste, or type the value into the field by
hand.

### Karavan re-sorts lines in `application.properties`
**Cause:** it normalizes the file when you edit it.
**Fix:** none needed. Order does not matter in a properties file.

### Karavan changed my YAML after I only opened it
**Cause:** opening a route in the **Designer** re-serializes it: long strings become folded blocks, keys are reordered and
spacing changes. After opening `consume-conversation-events.camel.yaml` its size went from 6,034 to 6,086 bytes with the same meaning.
**Fix:** none needed for behaviour. It does show up as a change in the Source tab and in `git diff`. If you want the file
exactly as in Git, press **Pull** (it replaces Karavan's copy).

### The size or the status of a file looks wrong right after I edited it
**Cause:** the Source list refreshes late. After pasting `application.properties` it showed 3 bytes for a 3,425-byte file.
**Fix:** reload the page. Check the content itself in the editor, not the size column.

### What does work in the code editor
Typing long text does not (see above), but these do:
- Select all and paste: `Cmd+A`, `Cmd+V`. The reliable way to replace a whole file.
- `Ctrl+G`, type a line number, `Enter`: go to a line.
- `End`, `Home`, `Shift+End`, `Alt+Shift+Left`: move and select, for a one-word change such as `groovy` → `simple`.
- Typing a short plain value, or one new line of a properties file, then `Esc` to close the suggestion box.
Avoid pressing `Enter` while the suggestion box is open: it accepts the suggestion.

### The Topology shows `{{app.topic}}` instead of the topic name
**Cause:** the Designer draws the value as written. The placeholder is resolved at runtime from `application.properties`.
**Fix:** none. The log shows the real topic (`Subscribed to topic(s): ...`).

---

## Git and Karavan

### **Push** is disabled
**Cause:** it needs files selected.
**Fix:** tick the files (header checkbox selects all), then Push.

### Karavan's commits show `admin <admin@platform.platform>`
**Cause:** that is the identity Karavan uses for its own commits.
**Fix:** none. Your own commits (docs, compose, scripts) use your Git identity.

### **Pull** warns "rewrites all non-committed code in the project"
**Cause:** Pull replaces Karavan's copy of every file with Git's, so anything you changed in Karavan and did not push is lost.
**Fix:** push first if you want to keep it. If the files in Karavan came from this repo and were not touched since, Pull is
safe, and afterwards every file shows a commit time instead of `NEW` or `CHANGED`.

### Files show `NEW` or `CHANGED` in the Source tab
**Cause:** `NEW` is a file Karavan has that Git does not; `CHANGED` differs from Git. They are not committed yet.
**Fix:** **Push** them, or **Pull** to discard them.

### My local clone is behind after a Karavan push
**Fix:** `git pull --ff-only`. If you have local commits that Karavan has not
seen, push them, then press **Pull** in Karavan.

### `docs/`, `scripts/` or `tests/` appear as projects in Karavan
**Cause:** Karavan treats each top-level folder of the repository as a project.
**Fix:** ignore them. Do not push changes into them from Karavan.

---

### `DLQ reason=store-rejected field=sqlstate-22021` (or `22P05`)
**Cause:** the store refused the data for good. `22021` is a NUL (`\u0000`) in a chat text, which Postgres cannot hold in a `text`; `22P05`
is a NUL escape inside a `jsonb`. `field=stream-position` is another case: a **different** event arrived at a position that already holds one.
**Fix:** none is needed for the stream: the record is dead-lettered, its offset moves, nothing is half written. Only the SQLSTATE is logged
(never the row), so look the record up by its `topic`, `partition` and `offset` in Kafka.

### `DLQ reason=schema-id`
**Cause:** the record's registry header names a schema id that is not in `app.schema.allowedIds` (default `2`): the producer moved to a new
schema version, or the header is corrupt (an id with the top bit set reads as negative and is refused too).
**Fix:** if the new version is intended, add its id to `app.schema.allowedIds` on purpose, recreate the container, and replay the records.

### I rewound my group to the earliest offset and nothing came back
**Cause:** the topic's retention removed the old events, so "earliest" is not offset 0 and the old records are gone for good. The
dry run showed non-zero NEW-OFFSETs.
**Fix:** always run `--dry-run` first and compare. What the topic no longer holds can only be rebuilt from the event store.

### Tests: a script waits for something that already happened
**Cause:** `wait_until "..." 30 test "$(count)" -ge 1` expands `$(count)` once, before the wait starts, so the condition never changes.
**Fix:** pass the condition as a string that is evaluated every second (`wait_until "..." 30 '[ "$(count)" -ge 1 ]'`). The same trap
bites any loop that polls with a command substitution in its arguments.

### Tests: a piped answer never reaches the script, or `set -e` stops it silently
**Cause:** `docker exec -i` keeps the caller's stdin open and swallows what was piped to it (`echo no | script`).
**Fix:** run `docker exec` without `-i` when the command needs no input.

### Tests: the checker or a Docker command sees an empty folder
**Cause:** the macOS temp folder is not shared with the Docker VM.
**Fix:** keep files that a container must read inside the repo (`.devmode-local/`, git-ignored).



---

## Reset everything

Remove Karavan and its database. Projects are safe in Git:

```bash
./scripts/clean.sh
docker rm -f ms-ml-conv-history-consumer 2>/dev/null     # DevMode containers are not removed by clean.sh
./scripts/up.sh
```

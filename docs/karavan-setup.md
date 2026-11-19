# Karavan setup

How Apache Camel Karavan runs in this repository, and how it was set up.

Karavan is a visual designer for Camel integrations. You draw routes in a web
UI; it stores them as YAML in this Git repository and can run them in a
container on your machine ("DevMode").

## Architecture

```
                         your browser  ──►  http://localhost:9997
                                                  │
 ┌───────────────────── docker network: yape-local (created by ../cluster) ─────────────────────┐
 │                                                                                              │
 │  consumer-karavan  ──► consumer-karavan-db   (Postgres 18: Karavan's own state, volume karavan_postgres_data)
 │  (ghcr.io/apache/camel-karavan:4.22.2)                                                       │
 │     │  ─ Git over HTTPS ─► GitHub  (projects live in this repo; the token comes from .env)   │
 │     │  ─ /var/run/docker.sock ─► starts DevMode containers on your Docker engine             │
 │     │                                                                                        │
 │     └─ starts ─► ms-ml-conv-history-consumer  (ghcr.io/apache/camel-karavan-devmode:4.22.2)        │
 │                       └─ reads Kafka as kafka:29092 ──► yape-kafka (from ../cluster)         │
 │                                                                                              │
 │  consumer-registry (registry:2, host port 5556)  — target for images that Karavan builds     │
 └──────────────────────────────────────────────────────────────────────────────────────────────┘
```

Three services in `docker-compose.yml`:

| Service | Image | Purpose | Host port |
|---|---|---|---|
| `consumer-karavan` | `ghcr.io/apache/camel-karavan:4.22.2` | The designer and its API | `127.0.0.1:9997` |
| `consumer-karavan-db` | `postgres:18` | Karavan's database. Not published on the host. | none |
| `consumer-registry` | `registry:2` | Local image registry for Karavan builds | `127.0.0.1:5556` |

## Running next to ms-ml-camel-conv-history-bs

The two Karavan stacks run at the same time. Both join the shared network `yape-local` and the same Docker engine, so every name and
port that could collide is different:

| | `ms-ml-camel-conv-history-bs` | This repository |
|---|---|---|
| Karavan UI | `9998` | **`9997`** |
| Image registry | `5555` | **`5556`** |
| Consumer, host port (app port 8080 inside) | `8081` | **`8087`** |
| Karavan, its database, its registry (container and DNS name on `yape-local`) | `karavan`, `karavan-db`, `karavan-registry` | **`consumer-karavan`, `consumer-karavan-db`, `consumer-registry`** |
| Karavan project, and so the consumer's container | `conv-history-consumer` | **`ms-ml-conv-history-consumer`** |
| Kafka consumer group | `ms-ml-camel-conv-history-bs` | **`ms-ml-conv-history-consumer`** |
| Postgres volume of Karavan | `ms-ml-camel-conv-history-bs_karavan_postgres_data` | `ms-ml-conv-history-consumer_karavan_postgres_data` |

The DNS names matter most. On a shared network a service name is an alias, and two containers with the same alias both answer it: with
two Karavans each called `karavan-db`, one Karavan could read the other's projects. The ports the other stack uses besides these are
8084 (sync), 8086 (API), 8098 (fake Foundry) and, in the cluster, 8082, 8083, 8085 and 8090. Both consumers write the same three tables;
that is safe, because the inbox refuses a duplicate, but they do the same work twice.

If you change a port, check it first: `lsof -nP -iTCP:<port> -sTCP:LISTEN`.

## Prerequisites

1. **Docker** running.
2. **The shared cluster up**, because Karavan joins its network and reaches
   Kafka through it:
   ```bash
   ../cluster/scripts/up.sh
   ```
   Without it, `docker compose up` fails with
   `network yape-local declared as external, but could not be found`.
3. **A GitHub token** so Karavan can read and write this repository: a
   fine-grained personal access token limited to this repo with
   *Contents: read and write*.

## First-time setup

```bash
cp .env.example .env      # .env is git-ignored
```

Edit `.env` and set `GITHUB_MALMEIDAP_PAT` to the token. The other values in
`.env.example` have working defaults.

```bash
./scripts/up.sh           # checks Docker, the yape-local network and the token, then starts the stack
```

First start pulls images and takes about a minute. Open
<http://localhost:9997> and log in. The login credentials are not stored in
this repository.

Karavan is ready when `docker logs consumer-karavan` shows `karavan 4.22.2 on JVM ...
started` and `Projects loaded`.

## What each setting does

All in `docker-compose.yml`, service `karavan`:

| Variable | Value | Why |
|---|---|---|
| `KARAVAN_GIT_REPOSITORY` / `_USERNAME` / `_BRANCH` | from `.env` | Where projects are stored. |
| `KARAVAN_GIT_PASSWORD` | `${GITHUB_MALMEIDAP_PAT}` | The token. It is never written in the compose file. |
| `KARAVAN_DOCKER_NETWORK` | `yape-local` | Containers Karavan starts join the cluster network, so they resolve `kafka`, `redis` and the rest by service name. |
| `KARAVAN_DEVMODE_IMAGE` | `ghcr.io/apache/camel-karavan-devmode:4.22.2` | **Required.** Karavan's built-in default (`apache/camel-karavan-devmode`) does not exist on Docker Hub, so *Run* fails. See [troubleshooting](troubleshooting.md#error-starting-devmode-container--pull-access-denied). Keep its tag equal to the Karavan version. |
| `KARAVAN_CONTAINER_IMAGE_REGISTRY` | `registry:5000` | Where builds are pushed. |
| `KARAVAN_DATASOURCE_*` | `karavan` / `karavan` | Credentials for `consumer-karavan-db`. Local only, not exposed on the host. |

The mount `/var/run/docker.sock:/var/run/docker.sock` gives Karavan control of
your Docker engine. That is how it starts containers, and it means **anyone
who can use the Karavan UI is effectively root on this machine**. Port 9997 is
published on `127.0.0.1` only; keep it that way.

## How Karavan and Git work together

- Each **top-level folder** in the repository is a Karavan **project**
  (`ms-ml-conv-history-consumer/` here). Karavan loads projects from Git at startup
  and when you press **Pull**.
- Edits in the UI are saved to Karavan's database first and show status `NEW`
  or *changed*. They are **not in Git** until you push.
- To commit: open the project, **Source** tab, tick the files (use the
  header checkbox to select all), click **Push**, enter a message. **Push stays
  disabled until at least one file is ticked.**
- Karavan commits as `admin <admin@platform.platform>`.
- After a push, update your local clone: `git pull --ff-only`.
- Because state is in Git, `./scripts/clean.sh` is safe: it removes containers
  and the Postgres volume, and the projects come back from Git.

> Folders that are not Karavan projects (`docs/`, `scripts/`, `tests/`) sit in the same
> repository. If Karavan lists them as projects after a Pull, ignore them or
> delete the entry in the UI; do not push changes into them from Karavan.

## Day-to-day scripts

| Script | Effect |
|---|---|
| `./scripts/up.sh` | Create and start the stack (checks prerequisites first). |
| `./scripts/stop.sh` | Stop containers; keep them and their data. |
| `./scripts/start.sh` | Start containers created earlier. |
| `./scripts/logs.sh` / `logs.sh -f` | Save logs to `logs_<timestamp>.txt` (git-ignored) / follow them. |
| `./scripts/clean.sh` | Remove containers **and** the Postgres volume. Projects stay in Git. |

DevMode containers started by Karavan (such as `ms-ml-conv-history-consumer`) are
managed from the Karavan UI, not by these scripts. `clean.sh` does not remove
them.

Development aids (they do not use Karavan; see the [consumer guide](conv-history-consumer.md#change-it)):

| Script | Effect |
|---|---|
| `./scripts/devmode-local.sh <project> [NAME=value ...]` | Run a project folder of this repo in the DevMode image, on the cluster network. `stop <project>` removes it. |
| `./scripts/test-consumer.sh` | End-to-end test of `ms-ml-conv-history-consumer` on a throwaway topic (37 checks). It touches only its own `t-` rows. |
| `./scripts/test-all.sh` | Every test of the repository in turn, with one table at the end ([testing.md](testing.md)). |

## Files Karavan generates in a project

When you create a project, Karavan adds template files. For
`ms-ml-conv-history-consumer`:

| File | Notes |
|---|---|
| `application.properties` | Camel settings, dependencies, the datasource. Changed from the template; see the [consumer guide](conv-history-consumer.md#applicationproperties-changes-from-the-template). |
| `consume-conversation-events.camel.yaml`, `store-conversation-history.camel.yaml`, `handle-errors-and-dead-letters.camel.yaml` | The three routes: Receive, Store, Dead-letter (with the error rules). |
| `*.java` | Small Java helpers (decoder, store outcome, offset commit, store refusal). Karavan keeps `.java` files in the project like any other file, and DevMode compiles them at startup. |
| `docker-compose.yaml` | Karavan reads it when it runs the project: the published port (8087) and the container name. The port must be plain `host:container`: Karavan's parser fails on `127.0.0.1:8087:8080` and Run does nothing. Edit the port here, then recreate the container. |
| `PROJECT.md` | The route table, the key and the file list of the project, shown with it in Karavan (excluded from the run by `camel.jbang.excludes=*.md`). |

The "Kafka" starter also generates `route-processing`, `route-send` and
`default-error-handler`. They were demo routes unrelated to the consumer and
were deleted.

## Rebuilding from scratch

1. `../cluster/scripts/up.sh`
2. `cp .env.example .env` and set the token.
3. `./scripts/up.sh`
4. Open <http://localhost:9997>, log in, open `ms-ml-conv-history-consumer`.
5. Click **Run at the top right of the page** (not the Run of the Containers tab; see
   [troubleshooting](troubleshooting.md#which-button-does-what)).

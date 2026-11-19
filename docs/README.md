# Documentation

Guides for the Apache Camel Karavan setup in this repository and for the Kafka consumer `ms-ml-conv-history-consumer` built with it.

| Guide | Read it when |
|---|---|
| [handoff.md](handoff.md) | **You are picking the work up.** What this is, where it came from, the rules the owner set, how to change it safely, what is open. |
| [design.md](design.md) | You want the picture first: the context, the containers, the three routes and their steps, the vocabulary, the legend and the Camel patterns used. |
| [kamelets.md](kamelets.md) | **You want to add another consumer.** What the Kamelet does, its parameters, a smallest consumer you can copy, and what happens when a route fails. |
| [conv-history-consumer.md](conv-history-consumer.md) | You want to know what the consumer does, how to run it, test it and replay messages. |
| [karavan-setup.md](karavan-setup.md) | You need to start Karavan, understand how it is wired, or rebuild it from scratch. |
| [testing.md](testing.md) | You want to run or add a test: what each one proves and what is not covered. |
| [troubleshooting.md](troubleshooting.md) | Something failed. Each entry lists the symptom, the cause and the fix. Every entry is a problem we actually hit. |
| [cheatsheet.md](cheatsheet.md) | You need a command: Docker, Kafka CLI, consumer-group offsets, Git sync. |

## Repository layout

```
.
├── docker-compose.yml          Karavan, its Postgres and an image registry
├── .env.example                Settings to copy to .env (git-ignored; holds the GitHub token)
├── scripts/                    up / stop / start / logs / clean for the Karavan stack, the local runner (devmode-local.sh),
│                               the tests (test-consumer.sh, test-camel.sh for the JUnit, test-all.sh runs them all),
│                               check-docs.py and scan-secrets.sh (run it before every push)
├── ms-ml-conv-history-consumer/      The Karavan project: the Kafka consumer that builds the conversation history
├── kamelets/                         Karavan's "Custom Kamelets" project: kafka-at-least-once-source, reusable by every consumer
├── kamelets/                   Karavan's "Custom Kamelets" project: kafka-at-least-once-source, reused by every consumer
├── tests/camel/consumer/       The JUnit module: it compiles the project's own Java files
└── docs/                       This documentation
```

Not in Git: `.env` (secrets), `.test-logs/` (the output of `test-all.sh`), `.devmode-local/` (working copies of the local runner) and
`temp/` (working notes).

The folders `ms-ml-conv-history-consumer/` and `kamelets/` are Karavan *projects*. Karavan commits to this same Git repository, so the project is never lost
when the local containers are removed.

## Conventions used in these guides

- Commands run from the repository root unless stated.
- No credentials appear in these docs or in the repository. The GitHub token lives in `.env`; the Karavan UI login is not defined here
  and must not be committed.
- Everything described is for **local development** against the shared local cluster (`../cluster`), with fake data.

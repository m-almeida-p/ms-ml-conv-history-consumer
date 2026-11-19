# ms-ml-conv-history-consumer

Builds the conversation history from the events topic, into three tables, like `ms-ml-conv-history-bs`. It checks each
record against the Kafka schema and writes the ones it cannot store to a log.

Everything below is explained, with diagrams, in [`docs/design.md`](../docs/design.md).

## Three routes, read in order

| Route | Steps |
|---|---|
| **1 Receive** | 1.1 Decode, 1.2 Route. The Kamelet source reads the topic and commits the offset when the route returns |
| **2 Store** | 2.1 Write, 2.2 Decide the outcome, 2.3 Log the outcome |
| **3 Dead-letter** | 3.1 Name the reason, 3.2 Prepare the row, 3.3 Write the dead letter, 3.4 Write the log |

Route 1 calls route 2 (step 1.2.2). A record either route cannot store goes to route 3.

In the **Topology** tab: `{{app.topic}}` is the events topic (a placeholder, resolved from `application.properties`),
`errorRules` are the error rules that send a record to route 3, and `#pgDataSource` is the database.

## Key

- **Numbers** are the same in the Designer, the docs and the logs: `2.1` is step 1 of route 2.
- **(Parentheses)** name the Enterprise Integration Pattern the step implements.
- **Note** says why. **Docs:** at the end of a note points to the section that explains it.
- Full legend and vocabulary: [`docs/design.md`](../docs/design.md#legend).

## Files

| File | Holds |
|---|---|
| `consume-conversation-events.camel.yaml` | Route 1 |
| `store-conversation-history.camel.yaml` | Route 2, with the SQL |
| `handle-errors-and-dead-letters.camel.yaml` | Error rules, and route 3 |
| `*.java` | The beans of steps 1.1, 2.2 and 3.2, and `StoreRefusal` (names a store refusal in the error rules) |
| `../kamelets/kafka-at-least-once-source.kamelet.yaml` | The Kamelet route 1 starts from: reads the topic, commits the offset after the route. Not in this folder: it is shared (Karavan's **Custom Kamelets** project) |
| `application.properties` | Settings (`app.*`), dependencies, datasource |

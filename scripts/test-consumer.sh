#!/usr/bin/env bash
# End-to-end test of the ms-ml-conv-history-consumer project against the local cluster.
#
#   ./scripts/test-consumer.sh
#
# It starts the project with scripts/devmode-local.sh on a THROWAWAY topic and consumer group, publishes
# about 30 records in the format Debezium writes (Schema Registry header + JSON, schema id 2), waits until
# all are handled, then checks:
#   - the three conversation tables (rows, roles, authors, summary counts),
#   - the dead-letter table (dead_letter.dead_letters): one row per poison record, with the exact reason and the record as it came,
#     and the dead-letter log: one line per record, without the record,
#   - that every offset was committed (no lag), so no poison record blocked a partition.
#
# It touches only rows whose conversation id starts with "t-" (it removes them first and afterwards) and its own topic.
# The real rows of the schema history (the tables of ms-ml-conv-history-bs) and the real topic are never
# touched. (An earlier version truncated the three tables, which would have wiped the real history the API serves.)
# Needs: the cluster up, the history migrations applied (ms-ml-conv-history-migrations:
# make migrations/up), python3.

# shellcheck source=./common.sh
source "$(dirname "${BASH_SOURCE[0]}")/common.sh"

PROJECT="ms-ml-conv-history-consumer"
RUN="$(date +%s)"
TOPIC="camel.test.${RUN}"
GROUP="camel-test-${RUN}"
CONTAINER="devmode-local-${PROJECT}"
PSQL=(docker exec -i yape-postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 -tA)
KAFKA=(docker exec -i yape-kafka)
FAILS=0

require_docker
require_cluster_network
command -v python3 >/dev/null || { echo "❌  python3 is required" >&2; exit 1; }

pass() { echo "  ok    $*"; }
fail() { echo "  FAIL  $*"; FAILS=$((FAILS + 1)); }
sql()  { "${PSQL[@]}" -c "$1"; }
check() { # description, expected, actual
  if [ "$2" = "$3" ]; then pass "$1"; else fail "$1: expected [$2] got [$3]"; fi
}

cleanup() {
  "${REPO_DIR}/scripts/devmode-local.sh" stop "${PROJECT}" >/dev/null 2>&1 || true
  sql "DELETE FROM history.conversation_summaries WHERE conversation_id LIKE 't-%';
       DELETE FROM history.conversation_messages  WHERE conversation_id LIKE 't-%';
       DELETE FROM history.conversation_inbox     WHERE conversation_id LIKE 't-%';
       DELETE FROM dead_letter.dead_letters       WHERE service LIKE 'camel-test-%';" >/dev/null 2>&1 || true
  "${KAFKA[@]}" kafka-topics --bootstrap-server kafka:29092 --delete --topic "${TOPIC}" >/dev/null 2>&1 || true
  "${KAFKA[@]}" kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group "${GROUP}" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "🧪  topic ${TOPIC}, group ${GROUP}"
sql "SELECT count(*) FROM information_schema.tables WHERE (table_schema='history' AND table_name IN ('conversation_inbox','conversation_messages','conversation_summaries')) OR (table_schema='dead_letter' AND table_name='dead_letters')" | grep -q '^4$' \
  || { echo "❌  the conversation or dead-letter tables are missing: run the migrations first (ms-ml-conv-history-migrations)" >&2; exit 1; }
sql "DELETE FROM history.conversation_summaries WHERE conversation_id LIKE 't-%';
     DELETE FROM history.conversation_messages  WHERE conversation_id LIKE 't-%';
     DELETE FROM history.conversation_inbox     WHERE conversation_id LIKE 't-%';
     DELETE FROM dead_letter.dead_letters       WHERE service LIKE 'camel-test-%';" >/dev/null
"${KAFKA[@]}" kafka-topics --bootstrap-server kafka:29092 --create --topic "${TOPIC}" --partitions 3 --replication-factor 1 >/dev/null 2>&1

echo "🚀  starting the consumer (the first start downloads dependencies, about a minute)"
"${REPO_DIR}/scripts/devmode-local.sh" "${PROJECT}" "APP_TOPIC=${TOPIC}" "APP_KAFKA_GROUPID=${GROUP}" >/dev/null
# Reads the log into a variable and matches it there: "docker logs | grep -q" fails under pipefail (SIGPIPE).
log_has() { # extended regex
  local out
  out="$(docker logs "${CONTAINER}" 2>&1 || true)"
  grep -qE "$1" <<< "${out}"
}
for _ in $(seq 1 90); do
  log_has 'Successfully joined group' && break
  if log_has 'Error starting Camel|FailedToCreateRoute|Compilation error'; then
    docker logs "${CONTAINER}" 2>&1 | tail -30
    echo "❌  the consumer did not start" >&2
    exit 1
  fi
  sleep 2
done
log_has 'Successfully joined group' || { echo "❌  the consumer did not join the group" >&2; exit 1; }

echo "📨  publishing records"
python3 -I - > /tmp/camel-test-records.$$ <<'PY'
import json, sys

def frame(schema_id, body):
    return b"\x00" + schema_id.to_bytes(4, "big") + body

def key(cid):
    return frame(1, json.dumps({"conversation_id": cid}).encode())

ACCOUNT = {"t-c1": "acc-t1", "t-c2": "acc-t2"}

def event(cid, seq, name, body, drop=(), **over):
    # body is the payload as an object, sent as a JSON string like Debezium does; pass payload=... to send it raw
    d = {
        "conversation_id": cid, "sequence": seq, "event_name": name, "event_version": 1,
        "payload": json.dumps(body), "metadata": "{}", "correlation_id": "corr-" + cid,
        "id_yape_account": ACCOUNT.get(cid), "actor": "user", "flow": "p2p",
        "occurred_at": "2026-11-18T10:00:00.000000Z",
    }
    d.update(over)
    for name_ in drop:
        d.pop(name_, None)
    return d

def put(cid, value_bytes):
    sys.stdout.buffer.write(key(cid) + b"\t" + value_bytes + b"\n")

def good(d, sid=2):
    put(d["conversation_id"] if isinstance(d.get("conversation_id"), str) else "t-x", frame(sid, json.dumps(d).encode()))

# --- stored -------------------------------------------------------------------------------------------
good(event("t-c1", 1, "UserMessageSent", {"text": "hola"}, occurred_at="2026-11-18T10:00:00.000000Z"))
good(event("t-c1", 2, "AgentMessageSent", {"text": "hola, en que te ayudo"}, actor="agent", occurred_at="2026-11-18T10:00:05.000000Z"))
good(event("t-c1", 3, "GuardrailResponseSent", {"response_text": "no puedo ayudarte con eso"}, actor="agent", occurred_at="2026-11-18T10:00:10.000000Z"))
good(event("t-c1", 4, "ToolCallSummary", {"tool": "lookup"}, actor="agent", occurred_at="2026-11-18T10:00:20.000000Z"))
good(event("t-c2", 1, "UserMessageSent", {}, occurred_at="2026-11-18T11:00:00.000000Z"))                                  # no text: invalid, inbox only
good(event("t-c2", 2, "UserMessageSent", {"text": "sin cuenta"}, id_yape_account=None, occurred_at="2026-11-18T11:00:01.000000Z"))  # no account: invalid
good(event("t-c2", 3, "AgentMessageSent", {"text": "hello"}, id_yape_account=None, actor="agent", occurred_at="2026-11-18T11:00:02.000000Z"))  # message, no summary
good(event("t-c2", 4, "UserMessageSent", {"text": ""}, occurred_at="2026-11-18T11:00:03.000000Z"))                       # empty text is valid
good(event("t-c2", 5, "UserMessageSent", {"text": "cortado éñ", "truncated": True}, occurred_at="2026-11-18T11:00:04.000000Z"))

# --- a redelivery, and a different event at a taken position ------------------------------------------------
good(event("t-c1", 1, "UserMessageSent", {"text": "hola"}, occurred_at="2026-11-18T10:00:00.000000Z"))                  # duplicate: nothing written
good(event("t-c1", 2, "AgentMessageSent", {"text": "OTHER TEXT"}, actor="agent", occurred_at="2026-11-18T10:00:05.000000Z"))  # store-rejected

# --- poison -------------------------------------------------------------------------------------------------
base = event("t-p", 1, "UserMessageSent", {"text": "x"}, id_yape_account="acc-p")
put("t-p", json.dumps(base).encode())                                                   # no-schema-header
good(base, sid=9)                                                                       # schema-id
put("t-p", b"\x00\x00\x00")                                                             # header
put("t-p", frame(2, b"{bad json"))                                                      # json
put("t-p", frame(2, b"[1,2]"))                                                          # json (not an object)
good(event("t-p", "1", "UserMessageSent", {"text": "x"}, id_yape_account="acc-p"))      # schema-violation: sequence is a string
good(event("t-p", 1, "UserMessageSent", {"text": "x"}, drop=("conversation_id",), id_yape_account="acc-p"))  # field-missing
good(event("t-p", 0, "UserMessageSent", {"text": "x"}, id_yape_account="acc-p"))        # field-range sequence 0
good(event("t-p", 3000000000, "UserMessageSent", {"text": "x"}, id_yape_account="acc-p"))   # field-range sequence too big
good(event("bad id", 1, "UserMessageSent", {"text": "x"}, id_yape_account="acc-p"))     # field-range conversation_id
good(event("t-p", 1, "UserMessageSent", {"text": "x"}, flow="P2P", id_yape_account="acc-p"))   # field-range flow
good(event("t-p", 1, "UserMessageSent", {"text": "x"}, actor="robot", id_yape_account="acc-p"))   # field-range actor
good(event("t-p", 1, "UserMessageSent", {"text": "x"}, occurred_at="yesterday", id_yape_account="acc-p"))   # occurred-at
good(event("t-p", 1, "UserMessageSent", {"text": "x"}, payload="not json", id_yape_account="acc-p"))   # payload
good(event("t-p", 1, "UserMessageSent", {"text": "x"}, drop=("metadata",), id_yape_account="acc-p"))   # field-missing metadata
good(event("t-p", 1, "U" * 129, {"text": "x"}, id_yape_account="acc-p"))               # field-range event_name

# --- the store refuses the data for good: Postgres cannot hold a NUL, in a text or anywhere in a jsonb ------------------
good(event("t-nul", 1, "UserMessageSent", {"text": "before\u0000after"}, id_yape_account="acc-p"))      # sqlstate-22021
good(event("t-nul", 2, "ToolCallSummary", {"tool": "a\u0000b"}, id_yape_account="acc-p"))                # sqlstate-22P05

# --- a tombstone ---------------------------------------------------------------------------------------------
put("t-c1", b"")
PY
TOTAL=$(grep -c '' /tmp/camel-test-records.$$)
"${KAFKA[@]}" kafka-console-producer --bootstrap-server kafka:29092 --topic "${TOPIC}" \
  --property parse.key=true --property "key.separator=$(printf '\t')" < /tmp/camel-test-records.$$ >/dev/null 2>&1
rm -f /tmp/camel-test-records.$$
echo "    ${TOTAL} records published"

echo "⏳  waiting for the consumer to handle them"
handled() { docker logs "${CONTAINER}" 2>&1 | grep -cE 'Event (stored|duplicate) conversation=|DLQ reason=|Tombstone skipped'; }
for _ in $(seq 1 45); do
  [ "$(handled)" -ge "${TOTAL}" ] && break
  # A failure no rule classifies is retried forever (like a database outage), so fail now instead of waiting.
  if log_has 'Failed delivery'; then
    echo "❌  a record hit an unexpected error; the consumer would retry it forever:" >&2
    docker logs "${CONTAINER}" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep 'Failed delivery' | head -1 | cut -c1-500 >&2 || true
    exit 1
  fi
  sleep 2
done
echo "    handled $(handled) of ${TOTAL}"
sleep 3   # let the last offset commits land

echo "🔎  tables (conversation_*)"
check "inbox rows"        "9"  "$(sql "SELECT count(*) FROM history.conversation_inbox WHERE conversation_id LIKE 't-%'")"
check "message rows"      "6"  "$(sql "SELECT count(*) FROM history.conversation_messages WHERE conversation_id LIKE 't-%'")"
check "summary rows"      "2"  "$(sql "SELECT count(*) FROM history.conversation_summaries WHERE conversation_id LIKE 't-%'")"
check "summary t-c1 (3 messages)" "3|2026-11-18 10:00:00+00|2026-11-18 10:00:10+00" \
  "$(sql "SET TIME ZONE 'UTC'; SELECT message_count||'|'||first_occurred_at||'|'||last_occurred_at FROM history.conversation_summaries WHERE conversation_id='t-c1'" | tail -1)"
check "summary t-c2 (2 counted messages)" "2" "$(sql "SELECT message_count FROM history.conversation_summaries WHERE conversation_id='t-c2'")"
check "user message: role and author" "appUser|acc-t1|hola" \
  "$(sql "SELECT role||'|'||author_id||'|'||text FROM history.conversation_messages WHERE conversation_id='t-c1' AND sequence=1")"
check "agent message: author is the maker" "appMaker|ms-ml-genai-agent-bs|hola, en que te ayudo" \
  "$(sql "SELECT role||'|'||author_id||'|'||text FROM history.conversation_messages WHERE conversation_id='t-c1' AND sequence=2")"
check "guardrail message takes response_text" "appMaker|no puedo ayudarte con eso" \
  "$(sql "SELECT role||'|'||text FROM history.conversation_messages WHERE conversation_id='t-c1' AND sequence=3")"
check "the conflicting event did not overwrite sequence 2" "hola, en que te ayudo" \
  "$(sql "SELECT text FROM history.conversation_messages WHERE conversation_id='t-c1' AND sequence=2")"
check "a non-chat event is in the inbox only" "1|0" \
  "$(sql "SELECT (SELECT count(*) FROM history.conversation_inbox WHERE conversation_id='t-c1' AND sequence=4)||'|'||(SELECT count(*) FROM history.conversation_messages WHERE conversation_id='t-c1' AND sequence=4)")"
check "events without text or account are inbox only" "2|0" \
  "$(sql "SELECT (SELECT count(*) FROM history.conversation_inbox WHERE conversation_id='t-c2' AND sequence IN (1,2))||'|'||(SELECT count(*) FROM history.conversation_messages WHERE conversation_id='t-c2' AND sequence IN (1,2))")"
check "a message with no account is stored but not counted" "1|appMaker" \
  "$(sql "SELECT count(*)||'|'||max(role) FROM history.conversation_messages WHERE conversation_id='t-c2' AND sequence=3")"
check "an empty text is a valid message" "1" "$(sql "SELECT count(*) FROM history.conversation_messages WHERE conversation_id='t-c2' AND sequence=4 AND text=''")"
check "truncated flag and unicode text" "true|cortado éñ" \
  "$(sql "SELECT truncated::text||'|'||text FROM history.conversation_messages WHERE conversation_id='t-c2' AND sequence=5")"
check "inbox keeps the Kafka coordinates" "${TOPIC}" "$(sql "SELECT DISTINCT kafka_topic FROM history.conversation_inbox WHERE conversation_id LIKE 't-%'")"
check "inbox rows are stamped projected" "0" "$(sql "SELECT count(*) FROM history.conversation_inbox WHERE conversation_id LIKE 't-%' AND projected_at IS NULL")"
check "payload is stored as JSON, not as a string" "object" "$(sql "SELECT jsonb_typeof(payload) FROM history.conversation_inbox WHERE conversation_id='t-c1' AND sequence=1")"

echo "🔎  dead-letter table"
DL="SELECT %s FROM dead_letter.dead_letters WHERE service='${GROUP}'"
got="$(sql "$(printf "${DL}" "reason||'='||count(*)") GROUP BY reason ORDER BY reason" | paste -sd' ' -)"
want="field-missing=2 field-range=6 header=1 json=2 no-schema-header=1 occurred-at=1 payload=1 schema-id=1 schema-violation=1 store-rejected=3"
check "poison records by reason, one row each" "${want}" "${got}"
check "every row names the topic, the partition and the offset of the record" "19" \
  "$(sql "$(printf "${DL}" 'count(*)') AND source_topic='${TOPIC}' AND source_partition >= 0 AND source_offset >= 0")"
check "every row starts open, on attempt 1, and has Kafka's timestamp" "19" \
  "$(sql "$(printf "${DL}" 'count(*)') AND status='open' AND attempt=1 AND source_timestamp IS NOT NULL")"
check "a framed record is kept byte for byte: the Schema Registry header, then the JSON" "1" \
  "$(sql "$(printf "${DL}" 'count(*)') AND reason='field-range' AND encode(substring(record_value from 1 for 5), 'hex')='0000000002' AND substring(record_value from 6 for 1)='{'::bytea" | awk '{print ($1>0)?1:0}')"
check "the key is kept too, framed like the value" "1" \
  "$(sql "$(printf "${DL}" 'count(*)') AND reason='field-range' AND encode(substring(record_key from 1 for 5), 'hex')='0000000001'" | awk '{print ($1>0)?1:0}')"
check "an unframed record is kept as it came" "1" \
  "$(sql "$(printf "${DL}" 'count(*)') AND reason='no-schema-header' AND substring(record_value from 1 for 1)<>'\\x00'::bytea")"
check "a poison record names its field" "1" "$(sql "$(printf "${DL}" 'count(*)') AND reason='field-range' AND field='conversation_id'")"
check "a NUL is refused for good, by SQLSTATE (22021 in a text, 22P05 in a jsonb)" "1 1" \
  "$(sql "$(printf "${DL}" 'count(*)') AND reason='store-rejected' AND field='sqlstate-22021'") $(sql "$(printf "${DL}" 'count(*)') AND reason='store-rejected' AND field='sqlstate-22P05'")"
check "the refused NUL records left nothing in any table" "0" \
  "$(sql "SELECT (SELECT count(*) FROM history.conversation_inbox WHERE conversation_id='t-nul') + (SELECT count(*) FROM history.conversation_messages WHERE conversation_id='t-nul')")"

echo "🔎  dead-letter log"
docker logs "${CONTAINER}" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | grep 'DLQ reason=' > "/tmp/camel-test-dlq.$$" || true
check "one log line per dead letter, with its coordinates and key" "19" \
  "$(grep -cE 'DLQ reason=[a-z-]+ field=.* topic=[^ ]+ partition=[0-9]+ offset=[0-9]+ key=.* detail=' "/tmp/camel-test-dlq.$$")"
check "the record itself is not in the log, only in the table" "0" "$(grep -c 'body=' "/tmp/camel-test-dlq.$$" || true)"
rm -f "/tmp/camel-test-dlq.$$"

echo "🔎  offsets"
lag="$("${KAFKA[@]}" kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "${GROUP}" 2>/dev/null | awk -v t="${TOPIC}" '$2==t && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')"
check "no lag: every offset was committed, none blocked" "0" "${lag}"

# ---------------------------------------------------------------------------------------------------------
# Phase 2: a transient failure is retried in place, never dead-lettered, and its offset does not move.
# The consumer is started against a database port nobody listens on, then against the real database.
# ---------------------------------------------------------------------------------------------------------
echo "🔌  outage: the database is unreachable"
"${REPO_DIR}/scripts/devmode-local.sh" stop "${PROJECT}" >/dev/null 2>&1 || true
TOPIC2="${TOPIC}-outage"
GROUP2="${GROUP}-outage"
GROUP3="${GROUP}-recovered"
"${KAFKA[@]}" kafka-topics --bootstrap-server kafka:29092 --create --topic "${TOPIC2}" --partitions 1 --replication-factor 1 >/dev/null 2>&1

python3 -I - <<'PY' | "${KAFKA[@]}" kafka-console-producer --bootstrap-server kafka:29092 --topic "${TOPIC2}" --property parse.key=true --property "key.separator=$(printf '\t')" >/dev/null 2>&1
import json, sys
event = {"conversation_id": "t-out", "sequence": 1, "event_name": "UserMessageSent", "event_version": 1,
         "payload": json.dumps({"text": "outage"}), "metadata": "{}", "correlation_id": "c", "id_yape_account": "acc-out",
         "actor": "user", "flow": "p2p", "occurred_at": "2026-11-18T12:00:00.000000Z"}
frame = lambda sid, b: b"\x00" + sid.to_bytes(4, "big") + b
# The refused record goes FIRST: with one partition, a record being retried blocks the ones behind it, and the point is to see the
# dead-letter insert meet the outage too.
sys.stdout.buffer.write(frame(1, b'{"conversation_id":"t-out-poison"}') + b"\t" + b"this record has no Schema Registry header" + b"\n")
sys.stdout.buffer.write(frame(1, b'{"conversation_id":"t-out"}') + b"\t" + frame(2, json.dumps(event).encode()) + b"\n")
PY

"${REPO_DIR}/scripts/devmode-local.sh" "${PROJECT}" "APP_TOPIC=${TOPIC2}" "APP_KAFKA_GROUPID=${GROUP2}" \
  "APP_DB_URL=jdbc:postgresql://db:5999/postgres?currentSchema=history" >/dev/null
for _ in $(seq 1 60); do
  out="$(docker logs "${CONTAINER}" 2>&1 | grep -c 'Failed delivery' || true)"
  [ "${out:-0}" -ge 2 ] && break
  sleep 2
done
check "the record is retried in place (at least 2 attempts logged)" "1" "$([ "${out:-0}" -ge 2 ] && echo 1 || echo 0)"
check "an outage is not dead-lettered, and a refused record waits too: no log line, no row" "0 0" \
  "$(docker logs "${CONTAINER}" 2>&1 | grep -c 'DLQ reason=' || true) $(sql "SELECT count(*) FROM dead_letter.dead_letters WHERE service='${GROUP2}'")"
check "nothing was stored" "0" "$(sql "SELECT count(*) FROM history.conversation_inbox WHERE conversation_id='t-out'")"
pending="$("${KAFKA[@]}" kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "${GROUP2}" 2>/dev/null \
  | awk -v t="${TOPIC2}" '$2==t && $5 ~ /^[0-9]+$/ && ($4=="-" || $4+0 < $5+0) {n++} END {print n+0}')"
check "its offset was not committed (the record is still pending)" "1" "${pending}"

echo "🔌  recovery: the database is back"
"${REPO_DIR}/scripts/devmode-local.sh" stop "${PROJECT}" >/dev/null 2>&1 || true
"${REPO_DIR}/scripts/devmode-local.sh" "${PROJECT}" "APP_TOPIC=${TOPIC2}" "APP_KAFKA_GROUPID=${GROUP3}" >/dev/null
for _ in $(seq 1 60); do
  log_has 'Event stored conversation=t-out' && break
  sleep 2
done
log_has 'Event stored conversation=t-out' && recovered=1 || recovered=0
check "the record is stored once the database is back" "1" "${recovered}"
check "and it is in the inbox" "1" "$(sql "SELECT count(*) FROM history.conversation_inbox WHERE conversation_id='t-out'")"
sleep 3
lag="$("${KAFKA[@]}" kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "${GROUP3}" 2>/dev/null | awk -v t="${TOPIC2}" '$2==t && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')"
check "and its offset is then committed" "0" "${lag}"
check "the refused record that met the outage is in the dead-letter table once the database is back, once" "no-schema-header=1" \
  "$(sql "SELECT reason||'='||count(*) FROM dead_letter.dead_letters WHERE service='${GROUP3}' GROUP BY reason")"
"${KAFKA[@]}" kafka-topics --bootstrap-server kafka:29092 --delete --topic "${TOPIC2}" >/dev/null 2>&1 || true
for g in "${GROUP2}" "${GROUP3}"; do "${KAFKA[@]}" kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group "$g" >/dev/null 2>&1 || true; done

# ---------------------------------------------------------------------------------------------------------
# Phase 3: an unexpected error (neither poison nor transient) is retried 3 times, then dead-lettered as
# reason=unexpected, and its offset moves, so a bug cannot hang a partition.
# A real one is cheap to make: point the consumer at a schema that has no tables ("relation does not exist").
# ---------------------------------------------------------------------------------------------------------
echo "💥  unexpected error: the tables are missing"
"${REPO_DIR}/scripts/devmode-local.sh" stop "${PROJECT}" >/dev/null 2>&1 || true
TOPIC4="${TOPIC}-unexpected"
GROUP4="${GROUP}-unexpected"
"${KAFKA[@]}" kafka-topics --bootstrap-server kafka:29092 --create --topic "${TOPIC4}" --partitions 1 --replication-factor 1 >/dev/null 2>&1
python3 -I - <<'PY' | "${KAFKA[@]}" kafka-console-producer --bootstrap-server kafka:29092 --topic "${TOPIC4}" --property parse.key=true --property "key.separator=$(printf '\t')" >/dev/null 2>&1
import json, sys
frame = lambda sid, b: b"\x00" + sid.to_bytes(4, "big") + b
for seq in (1, 2):
    event = {"conversation_id": "t-unx", "sequence": seq, "event_name": "UserMessageSent", "event_version": 1,
             "payload": json.dumps({"text": "unexpected %d" % seq}), "metadata": "{}", "correlation_id": "c", "id_yape_account": "acc-unx",
             "actor": "user", "flow": "p2p", "occurred_at": "2026-11-18T13:00:00.000000Z"}
    sys.stdout.buffer.write(frame(1, b'{"conversation_id":"t-unx"}') + b"\t" + frame(2, json.dumps(event).encode()) + b"\n")
PY
"${REPO_DIR}/scripts/devmode-local.sh" "${PROJECT}" "APP_TOPIC=${TOPIC4}" "APP_KAFKA_GROUPID=${GROUP4}" \
  "APP_DB_URL=jdbc:postgresql://db:5432/postgres?currentSchema=no_such_schema" >/dev/null
for _ in $(seq 1 60); do
  n="$(docker logs "${CONTAINER}" 2>&1 | grep -c 'DLQ reason=unexpected' || true)"
  [ "${n:-0}" -ge 2 ] && break
  sleep 2
done
docker logs "${CONTAINER}" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' > "/tmp/camel-test-unx.$$"
check "each unexpected record is dead-lettered once, as reason=unexpected" "2" "$(grep -c 'DLQ reason=unexpected' "/tmp/camel-test-unx.$$" || true)"
check "the dead-letter line names the exception class and carries its message" "1" \
  "$(grep 'DLQ reason=unexpected' "/tmp/camel-test-unx.$$" | head -1 | grep -c 'field=BadSqlGrammarException .* detail=[^ ]' || true)"
check "each was tried 3 more times before giving up (at least 4 failed deliveries per record)" "1" \
  "$([ "$(grep -c 'Failed delivery' "/tmp/camel-test-unx.$$" || true)" -ge 8 ] && echo 1 || echo 0)"
check "the record is not in the log" "0" "$(grep 'DLQ reason=unexpected' "/tmp/camel-test-unx.$$" | grep -c 'body=' || true)"
DL4="FROM dead_letter.dead_letters WHERE service='${GROUP4}'"
check "the table has one row per record, even though the store's schema is missing (the insert names its schema)" "2" "$(sql "SELECT count(*) ${DL4} AND reason='unexpected'")"
check "each row names the exception class and the 4 attempts" "2 2" \
  "$(sql "SELECT count(*) ${DL4} AND field='BadSqlGrammarException' AND exception_class LIKE '%BadSqlGrammarException'") $(sql "SELECT count(*) ${DL4} AND attempt=4")"
check "each row holds the original record: the Schema Registry header, then the JSON" "2" \
  "$(sql "SELECT count(*) ${DL4} AND encode(substring(record_value from 1 for 5), 'hex')='0000000002' AND convert_from(substring(record_value from 6), 'UTF8') LIKE '%t-unx%'")"
rm -f "/tmp/camel-test-unx.$$"
sleep 3
lag="$("${KAFKA[@]}" kafka-consumer-groups --bootstrap-server kafka:29092 --describe --group "${GROUP4}" 2>/dev/null | awk -v t="${TOPIC4}" '$2==t && $6 ~ /^[0-9]+$/ {s+=$6} END {print s+0}')"
check "their offsets were committed: a bug does not hang the partition" "0" "${lag}"
check "nothing was written" "0" "$(sql "SELECT count(*) FROM history.conversation_inbox WHERE conversation_id='t-unx'")"
"${KAFKA[@]}" kafka-topics --bootstrap-server kafka:29092 --delete --topic "${TOPIC4}" >/dev/null 2>&1 || true
"${KAFKA[@]}" kafka-consumer-groups --bootstrap-server kafka:29092 --delete --group "${GROUP4}" >/dev/null 2>&1 || true

echo
if [ "${FAILS}" -eq 0 ]; then echo "✅  all checks passed"; else echo "❌  ${FAILS} check(s) failed"; exit 1; fi

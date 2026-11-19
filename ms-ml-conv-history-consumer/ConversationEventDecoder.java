import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import org.apache.camel.BindToRegistry;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.Message;

/**
 * Turns one Kafka record of the conversation events topic into the values the store statement needs, and says
 * whether it is a chat message. It enforces, in this order:
 *
 *   1. the wire format: the Schema Registry header (zero byte + 4-byte schema id) and the schema id, which
 *      must be one of the allowed ids;
 *   2. the schema the registry holds for that id (a JSON Schema), so a type that does not fit is refused;
 *   3. the consumer's own contract, the rules of ms-ml-conv-history-bs's decoder (the registered schema has no
 *      "required" list, so it alone would let a record without a conversation_id through);
 *   4. the projection of the three chat events into a message.
 *
 * A record that can never be stored throws PoisonRecordException and the route dead-letters it. A failure that a
 * retry can fix (the registry is down) throws UncheckedIOException, which the route retries in place, forever.
 *
 * The decoder is registered as the bean "decoder" by its annotation. Its settings are the app.schema.* values of
 * application.properties, read on first use. It leaves the exchange body as a Map with the keys the SQL
 * statement binds.
 */
@BindToRegistry("decoder")
public class ConversationEventDecoder {

    /** Strict, like the reference's json.Unmarshal: anything after the top-level value is an error, not ignored. */
    private static final ObjectMapper JSON = new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private static final Pattern IDENTIFIER = Pattern.compile("^[A-Za-z0-9._:-]{1,128}$");
    private static final Pattern FLOW = Pattern.compile("^[a-z][a-z0-9_]{0,63}$");
    private static final Set<String> ACTORS = Set.of("user", "agent", "system");
    private static final int MAX_NAME_LENGTH = 128;
    private static final String MAKER_AUTHOR_ID = "ms-ml-genai-agent-bs";
    /** The most of a record the dead-letter log carries. */
    private static final int MAX_LOGGED_CHARS = 16384;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<Integer, JsonSchema> schemas = new ConcurrentHashMap<>();

    private String registryUrl = "http://schema-registry:8081";
    private Set<Integer> allowedSchemaIds = Set.of();
    private boolean requireFraming = true;

    // ---- settings: the app.schema.* values of application.properties ----

    private volatile boolean configured;

    private void configure(Exchange exchange) {
        if (configured) {
            return;
        }
        synchronized (this) {
            if (!configured) {
                CamelContext context = exchange.getContext();
                setRegistryUrl(context.resolvePropertyPlaceholders("{{app.schema.registryUrl}}"));
                setAllowedSchemaIds(context.resolvePropertyPlaceholders("{{app.schema.allowedIds}}"));
                setRequireFraming(Boolean.parseBoolean(context.resolvePropertyPlaceholders("{{app.schema.requireFraming}}")));
                configured = true;
            }
        }
    }

    public void setRegistryUrl(String registryUrl) {
        this.registryUrl = registryUrl.endsWith("/") ? registryUrl.substring(0, registryUrl.length() - 1) : registryUrl;
    }

    /** Comma separated schema ids, for example "2" or "2,5". */
    public void setAllowedSchemaIds(String ids) {
        Set<Integer> parsed = new TreeSet<>();
        for (String id : ids.split(",")) {
            if (!id.isBlank()) {
                parsed.add(Integer.parseInt(id.trim()));
            }
        }
        this.allowedSchemaIds = parsed;
    }

    public void setRequireFraming(boolean requireFraming) {
        this.requireFraming = requireFraming;
    }

    // ---- the processor the route calls ----

    public void decode(Exchange exchange) throws PoisonRecordException {
        configure(exchange);
        Message in = exchange.getMessage();
        byte[] value = in.getBody(byte[].class);
        exchange.setProperty("rawText", printable(value));
        // The key is framed like the value (header + {"conversation_id": ...}), so show it readable too.
        Object key = in.getHeader("CamelKafkaKey");
        exchange.setProperty("rawKey", key == null ? "" : printable(key instanceof byte[] b ? b : key.toString().getBytes(StandardCharsets.UTF_8)));
        exchange.setProperty("invalid", false);
        in.setHeader("Tombstone", false);

        if (value == null || value.length == 0) {
            in.setHeader("Tombstone", true);
            return;
        }

        // 1. wire format
        boolean framed = false;   // not schemaId >= 0: a header whose id has the top bit set reads as a negative int, and is still a header
        int schemaId = -1;
        byte[] json = value;
        if (value[0] == 0) {
            if (value.length < 5) {
                throw poison(exchange, "header", "", "the value is shorter than its schema registry header");
            }
            framed = true;
            schemaId = ByteBuffer.wrap(value, 1, 4).getInt();
            json = Arrays.copyOfRange(value, 5, value.length);
        } else if (requireFraming) {
            throw poison(exchange, "no-schema-header", "", "the value does not start with the Schema Registry magic byte");
        }

        String text = new String(json, StandardCharsets.UTF_8).trim();
        if (text.isEmpty() || text.equals("null")) {
            in.setHeader("Tombstone", true);
            return;
        }

        if (framed && !allowedSchemaIds.contains(schemaId)) {
            throw poison(exchange, "schema-id", "", "schema id " + schemaId + " is not one of the allowed ids " + allowedSchemaIds);
        }

        JsonNode root;
        try {
            root = JSON.readTree(text);
        } catch (JsonProcessingException e) {
            throw poison(exchange, "json", "", "the value is not valid JSON");
        }
        if (root == null || !root.isObject()) {
            throw poison(exchange, "json", "", "the value is not a JSON object");
        }

        // 2. the registered schema
        if (framed) {
            Set<ValidationMessage> errors = schemaFor(schemaId).validate(root);
            if (!errors.isEmpty()) {
                ValidationMessage first = errors.iterator().next();
                throw poison(exchange, "schema-violation", leafName(first.getInstanceLocation().toString()),
                        errors.size() + " violation(s) of schema " + schemaId + ", first: " + first.getMessage());
            }
        }

        // 3. the consumer's contract, in the order of the Go decoder: types, missing, range, documents, time
        for (String name : new String[] {"conversation_id", "event_name", "correlation_id", "id_yape_account", "actor", "flow", "occurred_at"}) {
            JsonNode node = root.get(name);
            if (node != null && !node.isNull() && !node.isTextual()) {
                throw poison(exchange, "field-malformed", name, "expected a string");
            }
        }
        for (String name : new String[] {"sequence", "event_version"}) {
            JsonNode node = root.get(name);
            if (node != null && !node.isNull() && !(node.isIntegralNumber() && node.canConvertToLong())) {
                throw poison(exchange, "field-malformed", name, "expected an integer");
            }
        }

        String conversationId = text(root, "conversation_id");
        String eventName = text(root, "event_name");
        String occurredAtText = text(root, "occurred_at");
        if (conversationId.isEmpty()) {
            throw poison(exchange, "field-missing", "conversation_id", "a required field is missing");
        }
        if (isAbsent(root, "sequence")) {
            throw poison(exchange, "field-missing", "sequence", "a required field is missing");
        }
        if (eventName.isEmpty()) {
            throw poison(exchange, "field-missing", "event_name", "a required field is missing");
        }
        if (isAbsent(root, "event_version")) {
            throw poison(exchange, "field-missing", "event_version", "a required field is missing");
        }
        if (occurredAtText.isEmpty() && isAbsent(root, "occurred_at")) {
            throw poison(exchange, "field-missing", "occurred_at", "a required field is missing");
        }

        long sequence = root.get("sequence").asLong();
        long eventVersion = root.get("event_version").asLong();
        String idYapeAccount = text(root, "id_yape_account");
        String correlationId = text(root, "correlation_id");
        String actor = text(root, "actor");
        String flow = text(root, "flow");

        if (sequence < 1 || sequence > Integer.MAX_VALUE) {
            throw poison(exchange, "field-range", "sequence", "outside 1 to " + Integer.MAX_VALUE);
        }
        if (eventVersion < 1 || eventVersion > Integer.MAX_VALUE) {
            throw poison(exchange, "field-range", "event_version", "outside 1 to " + Integer.MAX_VALUE);
        }
        if (!IDENTIFIER.matcher(conversationId).matches()) {
            throw poison(exchange, "field-range", "conversation_id", "not 1 to 128 of letters, digits and . _ : -");
        }
        if (!idYapeAccount.isEmpty() && !IDENTIFIER.matcher(idYapeAccount).matches()) {
            throw poison(exchange, "field-range", "id_yape_account", "not 1 to 128 of letters, digits and . _ : -");
        }
        if (!flow.isEmpty() && !FLOW.matcher(flow).matches()) {
            throw poison(exchange, "field-range", "flow", "does not match ^[a-z][a-z0-9_]{0,63}$");
        }
        if (eventName.codePointCount(0, eventName.length()) > MAX_NAME_LENGTH) {
            throw poison(exchange, "field-range", "event_name", "longer than " + MAX_NAME_LENGTH + " characters");
        }
        if (!actor.isEmpty() && !ACTORS.contains(actor)) {
            throw poison(exchange, "field-range", "actor", "not one of " + ACTORS);
        }

        JsonNode payload = document(exchange, root, "payload");
        JsonNode metadata = document(exchange, root, "metadata");

        OffsetDateTime occurredAt;
        try {
            occurredAt = OffsetDateTime.parse(occurredAtText).withOffsetSameInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            throw poison(exchange, "occurred-at", "occurred_at", "not an RFC 3339 time");
        }

        // 4. the projection: which events are chat messages, and what their text and author are
        String role = null;
        String textField = null;
        switch (eventName) {
            case "UserMessageSent" -> {
                role = "appUser";
                textField = "text";
            }
            case "AgentMessageSent" -> {
                role = "appMaker";
                textField = "text";
            }
            case "GuardrailResponseSent" -> {
                role = "appMaker";
                textField = "response_text";
            }
            default -> {
                // not part of the chat: stored in the inbox only
            }
        }

        boolean projectable = false;
        boolean invalid = false;
        String messageText = null;
        String authorId = null;
        boolean truncated = false;
        if (role != null) {
            JsonNode textNode = payload.isObject() ? payload.get(textField) : null;
            JsonNode truncatedNode = payload.isObject() ? payload.get("truncated") : null;
            if (textNode == null || !textNode.isTextual()) {
                invalid = true;     // an event that should project but lacks its text is stored without a message
            } else if (truncatedNode != null && !truncatedNode.isNull() && !truncatedNode.isBoolean()) {
                invalid = true;
            } else if (role.equals("appUser") && idYapeAccount.isEmpty()) {
                invalid = true;     // a user message needs the account as its author
            } else {
                projectable = true;
                messageText = textNode.asText();
                authorId = role.equals("appUser") ? idYapeAccount : MAKER_AUTHOR_ID;
                truncated = truncatedNode != null && truncatedNode.asBoolean(false);
            }
        }

        Map<String, Object> event = new LinkedHashMap<>();
        event.put("conversationId", conversationId);
        event.put("sequence", (int) sequence);
        event.put("eventName", eventName);
        event.put("eventVersion", (int) eventVersion);
        event.put("payload", payload.toString());
        event.put("metadata", metadata.toString());
        event.put("correlationId", blankToNull(correlationId));
        event.put("idYapeAccount", blankToNull(idYapeAccount));
        event.put("actor", blankToNull(actor));
        event.put("flow", blankToNull(flow));
        event.put("occurredAt", occurredAt);
        event.put("kafkaTopic", in.getHeader("CamelKafkaTopic", String.class));
        event.put("kafkaPartition", in.getHeader("CamelKafkaPartition", Integer.class));
        event.put("kafkaOffset", in.getHeader("CamelKafkaOffset", Long.class));
        event.put("projectable", projectable);
        event.put("role", role);
        event.put("authorId", authorId);
        event.put("text", messageText);
        event.put("truncated", truncated);

        exchange.setProperty("invalid", invalid);
        in.setHeader("conversationId", conversationId);
        in.setHeader("sequence", (int) sequence);
        in.setBody(event);
    }

    // ---- helpers ----

    /** Records why the record is refused for the dead-letter log, then builds the exception. */
    private static PoisonRecordException poison(Exchange exchange, String reason, String field, String detail) {
        exchange.setProperty("dlqReason", reason);
        exchange.setProperty("dlqField", field);
        exchange.setProperty("dlqDetail", detail);
        return new PoisonRecordException(exchange, reason, field, detail);
    }

    /** The text of a string field; an absent or null field is the empty string. */
    private static String text(JsonNode root, String name) {
        JsonNode node = root.get(name);
        return node == null || node.isNull() ? "" : node.asText();
    }

    private static boolean isAbsent(JsonNode root, String name) {
        JsonNode node = root.get(name);
        return node == null || node.isNull();
    }

    private static String blankToNull(String value) {
        return value.isEmpty() ? null : value;
    }

    /**
     * Reads the payload or metadata column. Debezium sends JSONB as a string that holds JSON; an object is
     * accepted as it is. The result must be a JSON object or array.
     */
    private static JsonNode document(Exchange exchange, JsonNode root, String name) throws PoisonRecordException {
        JsonNode node = root.get(name);
        if (node == null || node.isNull()) {
            throw poison(exchange, "field-missing", name, "a required field is missing");
        }
        JsonNode doc = node;
        if (node.isTextual()) {
            try {
                doc = JSON.readTree(node.asText().trim());
            } catch (JsonProcessingException e) {
                throw poison(exchange, name, name, "is not valid JSON");
            }
        }
        if (doc == null || !(doc.isObject() || doc.isArray())) {
            throw poison(exchange, name, name, "is not a JSON object or array");
        }
        return doc;
    }

    /** The schema the registry holds for an id, fetched once and kept. Ids never change, so there is nothing to refresh. */
    private JsonSchema schemaFor(int id) {
        JsonSchema cached = schemas.get(id);
        if (cached != null) {
            return cached;
        }
        try {
            HttpResponse<String> response = http.send(
                    HttpRequest.newBuilder(URI.create(registryUrl + "/schemas/ids/" + id)).timeout(Duration.ofSeconds(5)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                // Being unable to validate is not the record's fault: an outage, or an allowed id the registry
                // does not know (a setting error). The route retries these forever, with a WARN each time, and
                // never dead-letters the record for it. They are thrown as UncheckedIOException, which the
                // route's retry-forever rule matches.
                throw new IOException("the schema registry answered " + response.statusCode() + " for schema id " + id);
            }
            JsonNode body = JSON.readTree(response.body());
            if (!"JSON".equals(body.path("schemaType").asText("AVRO"))) {
                throw new IOException("schema id " + id + " is not a JSON Schema");
            }
            JsonSchema schema = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7).getSchema(body.get("schema").asText());
            schemas.put(id, schema);
            return schema;
        } catch (IOException e) {
            throw new UncheckedIOException("cannot validate: schema id " + id + " is not available from the schema registry: " + e.getMessage(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new UncheckedIOException("interrupted while reading schema id " + id, new IOException(e));
        }
    }

    /** "$.sequence" becomes "sequence". */
    private static String leafName(String location) {
        int dot = location.lastIndexOf('.');
        return dot < 0 ? location : location.substring(dot + 1);
    }

    /** The record as readable text for the dead-letter log: control characters (the header's zero bytes) are shown as \xNN. */
    static String printable(byte[] value) {
        if (value == null) {
            return "";
        }
        String text = new String(value, StandardCharsets.UTF_8);
        StringBuilder out = new StringBuilder(Math.min(text.length(), MAX_LOGGED_CHARS) + 16);
        for (int i = 0; i < text.length(); i++) {
            if (out.length() >= MAX_LOGGED_CHARS) {
                out.append("...(truncated, ").append(text.length()).append(" chars)");
                break;
            }
            char c = text.charAt(i);
            if (c < 0x20 && c != '\t' && c != '\n' || c == 0x7f) {
                out.append(String.format("\\x%02x", (int) c));
            } else {
                out.append(c);
            }
        }
        return out.toString();
    }
}

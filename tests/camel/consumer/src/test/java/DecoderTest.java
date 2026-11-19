import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The decoder against the real Schema Registry of the cluster (schema id 2, read only): every field at its edges, the
 * shapes a producer could get wrong, text that is hard to carry, and what is a tombstone.
 */
class DecoderTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private ConversationEventDecoder decoder;
    private DefaultCamelContext context;

    @BeforeEach
    void setUp() {
        decoder = new ConversationEventDecoder();
        context = contextFor(System.getenv().getOrDefault("REGISTRY_URL", "http://schema-registry:8081"), true);
    }

    /** A Camel context with the decoder's settings (the app.schema.* values of application.properties). */
    private static DefaultCamelContext contextFor(String registryUrl, boolean requireFraming) {
        DefaultCamelContext context = new DefaultCamelContext();
        java.util.Properties p = new java.util.Properties();
        p.setProperty("app.schema.registryUrl", registryUrl);
        p.setProperty("app.schema.allowedIds", "2");
        p.setProperty("app.schema.requireFraming", Boolean.toString(requireFraming));
        context.getPropertiesComponent().setOverrideProperties(p);
        return context;
    }

    // ---- building records -------------------------------------------------------------------------------------------------

    private static Map<String, Object> event() {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("conversation_id", "conv-1");
        e.put("sequence", 1);
        e.put("event_name", "UserMessageSent");
        e.put("event_version", 1);
        e.put("payload", "{\"text\":\"hola\"}");
        e.put("metadata", "{}");
        e.put("correlation_id", "corr-1");
        e.put("id_yape_account", "acc-1");
        e.put("actor", "user");
        e.put("flow", "p2p");
        e.put("occurred_at", "2026-11-18T10:00:00.000000Z");
        return e;
    }

    private static byte[] framed(int schemaId, byte[] json) {
        byte[] out = new byte[5 + json.length];
        out[0] = 0;
        ByteBuffer.wrap(out).putInt(1, schemaId);
        System.arraycopy(json, 0, out, 5, json.length);
        return out;
    }

    private static byte[] framed(Map<String, Object> event) throws Exception {
        return framed(2, JSON.writeValueAsBytes(event));
    }

    private Exchange exchange(byte[] value) {
        Exchange exchange = new DefaultExchange(context);
        exchange.getMessage().setBody(value);
        exchange.getMessage().setHeader("CamelKafkaTopic", "t");
        exchange.getMessage().setHeader("CamelKafkaPartition", 3);
        exchange.getMessage().setHeader("CamelKafkaOffset", 42L);
        return exchange;
    }

    private Exchange decoded(Map<String, Object> event) throws Exception {
        Exchange exchange = exchange(framed(event));
        decoder.decode(exchange);
        return exchange;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> body(Exchange exchange) {
        return exchange.getMessage().getBody(Map.class);
    }

    private PoisonRecordException refused(Map<String, Object> event) throws Exception {
        return assertThrows(PoisonRecordException.class, () -> decoded(event));
    }

    private static void assertReason(PoisonRecordException e, String reason, String field) {
        assertEquals(reason, e.getReason(), e.getMessage());
        assertEquals(field, e.getField(), e.getMessage());
    }

    private static String repeat(String s, int n) {
        return s.repeat(n);
    }

    // ---- the happy shape --------------------------------------------------------------------------------------------------

    @Test
    void aUserMessageBecomesAMessageWithTheAccountAsAuthor() throws Exception {
        Map<String, Object> row = body(decoded(event()));

        assertEquals("conv-1", row.get("conversationId"));
        assertEquals(1, row.get("sequence"));
        assertEquals(true, row.get("projectable"));
        assertEquals("appUser", row.get("role"));
        assertEquals("acc-1", row.get("authorId"));
        assertEquals("hola", row.get("text"));
        assertEquals(false, row.get("truncated"));
        assertEquals("t", row.get("kafkaTopic"));
        assertEquals(3, row.get("kafkaPartition"));
        assertEquals(42L, row.get("kafkaOffset"));
    }

    @Test
    void anAgentMessageHasTheMakersFixedAuthorAndTheGuardrailUsesResponseText() throws Exception {
        Map<String, Object> agent = event();
        agent.put("event_name", "AgentMessageSent");
        agent.put("actor", "agent");
        Map<String, Object> guard = event();
        guard.put("event_name", "GuardrailResponseSent");
        guard.put("payload", "{\"response_text\":\"no puedo\"}");

        assertEquals("ms-ml-genai-agent-bs", body(decoded(agent)).get("authorId"));
        Map<String, Object> g = body(decoded(guard));
        assertEquals("appMaker", g.get("role"));
        assertEquals("no puedo", g.get("text"));
    }

    @Test
    void anEventThatIsNotChatIsStoredInTheInboxOnly() throws Exception {
        for (String name : new String[] {"ToolCallSummary", "usermessagesent", "USERMESSAGESENT", "UserMessageSent ", "UserMessage"}) {
            Map<String, Object> e = event();
            e.put("event_name", name);

            Map<String, Object> row = body(decoded(e));

            assertEquals(false, row.get("projectable"), name);
            assertNull(row.get("role"));
        }
    }

    @Test
    void thePayloadIsAStringOfJsonHoldingAnObjectOrAnArray() throws Exception {
        Map<String, Object> asArray = event();
        asArray.put("event_name", "ToolCallSummary");
        asArray.put("payload", "[1,2]");

        assertEquals("[1,2]", body(decoded(asArray)).get("payload"));
    }

    @Test
    void aPayloadThatIsAnObjectIsRefusedByTheRegisteredSchemaButAcceptedWhenUnframed() throws Exception {
        Map<String, Object> asObject = event();
        asObject.put("payload", Map.of("text", "obj"));

        assertReason(refused(asObject), "schema-violation", "payload");

        ConversationEventDecoder plain = new ConversationEventDecoder();
        DefaultCamelContext other = contextFor("http://schema-registry:8081", false);
        Exchange exchange = new DefaultExchange(other);
        exchange.getMessage().setBody(JSON.writeValueAsBytes(asObject));
        plain.decode(exchange);
        assertEquals("obj", body(exchange).get("text"), "the decoder's own rule accepts an object, as the Go decoder does");
    }

    // ---- the text ---------------------------------------------------------------------------------------------------------

    static Stream<String> hardTexts() {
        return Stream.of("", " ", "  leading and trailing  ", "tab\there", "line\nbreak\r\n", "👍🏽", "👨‍👩‍👧",
                "é vs é", "שלום مرحبا", "中文測試", "  ", "​‍﻿", "back\\slash \"quote\"",
                "{\"json\":true}", "'; DROP TABLE x;--", "<script>alert(1)</script>", "x".repeat(100_000), "á".repeat(50_000));
    }

    @ParameterizedTest(name = "text #{index}")
    @MethodSource("hardTexts")
    void hardTextIsKeptExactly(String text) throws Exception {
        Map<String, Object> e = event();
        e.put("payload", JSON.writeValueAsString(Map.of("text", text)));

        assertEquals(text, body(decoded(e)).get("text"));
    }

    @Test
    void aTextWithNulPassesTheDecoderAndIsTheStoresProblem() throws Exception {
        Map<String, Object> e = event();
        e.put("payload", JSON.writeValueAsString(Map.of("text", "before\u0000after")));

        assertEquals("before\u0000after", body(decoded(e)).get("text"));
    }

    @Test
    void aTextThatIsNotAStringMakesTheEventInvalidNotAMessage() throws Exception {
        for (String payload : new String[] {"{\"text\":5}", "{\"text\":true}", "{\"text\":null}", "{\"text\":[\"a\"]}", "{}", "[]", "{\"Text\":\"x\"}", "{\"response_text\":\"x\"}"}) {
            Map<String, Object> e = event();
            e.put("payload", payload);

            Exchange exchange = decoded(e);

            assertEquals(false, body(exchange).get("projectable"), payload);
            assertEquals(true, exchange.getProperty("invalid"), payload);
        }
    }

    @Test
    void theTruncatedFlagMustBeABooleanWhenPresent() throws Exception {
        for (String ok : new String[] {"{\"text\":\"x\",\"truncated\":true}", "{\"text\":\"x\",\"truncated\":false}", "{\"text\":\"x\",\"truncated\":null}", "{\"text\":\"x\"}"}) {
            Map<String, Object> e = event();
            e.put("payload", ok);
            assertEquals(true, body(decoded(e)).get("projectable"), ok);
        }
        for (String bad : new String[] {"{\"text\":\"x\",\"truncated\":\"true\"}", "{\"text\":\"x\",\"truncated\":1}", "{\"text\":\"x\",\"truncated\":{}}"}) {
            Map<String, Object> e = event();
            e.put("payload", bad);
            Exchange exchange = decoded(e);
            assertEquals(false, body(exchange).get("projectable"), bad);
            assertEquals(true, exchange.getProperty("invalid"), bad);
        }
    }

    @Test
    void aUserMessageWithoutAnAccountIsInvalidButAnAgentOneIsFine() throws Exception {
        Map<String, Object> user = event();
        user.remove("id_yape_account");
        Map<String, Object> agent = event();
        agent.remove("id_yape_account");
        agent.put("event_name", "AgentMessageSent");

        assertEquals(true, decoded(user).getProperty("invalid"));
        Exchange a = decoded(agent);
        assertEquals(false, a.getProperty("invalid"));
        assertNull(body(a).get("idYapeAccount"), "no account: null, not an empty string");
    }

    // ---- boundaries of the numbers ----------------------------------------------------------------------------------------

    @ParameterizedTest(name = "sequence {0}")
    @ValueSource(longs = {1, 2, 1_000_000, 2_147_483_647L})
    void aSequenceInRangeIsAccepted(long sequence) throws Exception {
        Map<String, Object> e = event();
        e.put("sequence", sequence);

        assertEquals((int) sequence, body(decoded(e)).get("sequence"));
    }

    @ParameterizedTest(name = "sequence {0}")
    @ValueSource(longs = {0, -1, -2_147_483_648L, 2_147_483_648L, 9_999_999_999L})
    void aSequenceOutOfRangeIsRefused(long sequence) throws Exception {
        Map<String, Object> e = event();
        e.put("sequence", sequence);

        PoisonRecordException refusal = refused(e);

        assertTrue(refusal.getReason().equals("field-range") || refusal.getReason().equals("schema-violation"), refusal.getMessage());
    }

    @Test
    void aSequenceThatIsNotAnIntegerIsRefused() throws Exception {
        for (Object bad : new Object[] {"1", 1.5, 1.0, true, Map.of(), java.util.List.of(1)}) {
            Map<String, Object> e = event();
            e.put("sequence", bad);

            PoisonRecordException refusal = refused(e);

            assertTrue(refusal.getReason().equals("field-malformed") || refusal.getReason().equals("schema-violation"), bad + " -> " + refusal.getMessage());
        }
    }

    @Test
    void theEventVersionHasTheSameBounds() throws Exception {
        for (long ok : new long[] {1, 2_147_483_647L}) {
            Map<String, Object> e = event();
            e.put("event_version", ok);
            assertEquals((int) ok, body(decoded(e)).get("eventVersion"));
        }
        for (long bad : new long[] {0, -1, 2_147_483_648L}) {
            Map<String, Object> e = event();
            e.put("event_version", bad);
            refused(e);
        }
    }

    // ---- boundaries of the names ------------------------------------------------------------------------------------------

    @Test
    void aConversationIdOfOneToHundredTwentyEightAllowedCharactersIsAccepted() throws Exception {
        for (String id : new String[] {"a", "A.b_c:d-e", "0", repeat("x", 128), "conv_v1_abc:def.ghi-jkl"}) {
            Map<String, Object> e = event();
            e.put("conversation_id", id);
            assertEquals(id, body(decoded(e)).get("conversationId"));
        }
    }

    @Test
    void aConversationIdOutsideThatIsRefusedWithTheFieldNamed() throws Exception {
        for (String id : new String[] {repeat("x", 129), "a b", "a/b", "café", "👍", "a\nb", "a;b", "a%b", "../x", "a\u0000b"}) {
            Map<String, Object> e = event();
            e.put("conversation_id", id);

            PoisonRecordException refusal = refused(e);

            assertEquals("conversation_id", refusal.getField(), id + " -> " + refusal.getMessage());
        }
    }

    @Test
    void anEmptyOrMissingConversationIdIsMissing() throws Exception {
        Map<String, Object> empty = event();
        empty.put("conversation_id", "");
        Map<String, Object> missing = event();
        missing.remove("conversation_id");
        Map<String, Object> nullId = event();
        nullId.put("conversation_id", null);

        assertReason(refused(empty), "field-missing", "conversation_id");
        assertReason(refused(missing), "field-missing", "conversation_id");
        assertReason(refused(nullId), "schema-violation", "conversation_id");   // the registered schema says string, and it is checked first
    }

    @Test
    void theEventNameIsLimitedToHundredTwentyEightCharactersNotBytes() throws Exception {
        Map<String, Object> exact = event();
        exact.put("event_name", repeat("👍", 128));   // 128 characters of 4 bytes each: allowed
        Map<String, Object> over = event();
        over.put("event_name", repeat("👍", 129));

        assertEquals(repeat("👍", 128), body(decoded(exact)).get("eventName"));
        assertReason(refused(over), "field-range", "event_name");
    }

    @Test
    void theFlowFollowsItsPattern() throws Exception {
        for (String ok : new String[] {"a", "p2p", "a_b_9", "x" + repeat("y", 63)}) {
            Map<String, Object> e = event();
            e.put("flow", ok);
            assertEquals(ok, body(decoded(e)).get("flow"));
        }
        for (String bad : new String[] {"P2P", "1abc", "a-b", "a b", "x" + repeat("y", 64), "_a", "é"}) {
            Map<String, Object> e = event();
            e.put("flow", bad);
            assertReason(refused(e), "field-range", "flow");
        }
    }

    @Test
    void anEmptyFlowIsNoFlow() throws Exception {
        Map<String, Object> e = event();
        e.put("flow", "");

        assertNull(body(decoded(e)).get("flow"));
    }

    @Test
    void theActorIsOneOfThreeOrAbsent() throws Exception {
        for (String ok : new String[] {"user", "agent", "system"}) {
            Map<String, Object> e = event();
            e.put("actor", ok);
            assertEquals(ok, body(decoded(e)).get("actor"));
        }
        for (String bad : new String[] {"robot", "User", "USER", " user"}) {
            Map<String, Object> e = event();
            e.put("actor", bad);
            assertReason(refused(e), "field-range", "actor");
        }
    }

    @Test
    void theAccountFollowsTheIdentifierRule() throws Exception {
        Map<String, Object> bad = event();
        bad.put("id_yape_account", repeat("a", 129));
        Map<String, Object> ok = event();
        ok.put("id_yape_account", repeat("a", 128));

        assertReason(refused(bad), "field-range", "id_yape_account");
        assertEquals(repeat("a", 128), body(decoded(ok)).get("idYapeAccount"));
    }

    // ---- the time ---------------------------------------------------------------------------------------------------------

    @Test
    void anOffsetTimeIsConvertedToUtcAndKeepsItsFraction() throws Exception {
        Map<String, Object> e = event();
        e.put("occurred_at", "2026-11-18T05:00:00.123456-05:00");

        OffsetDateTime at = (OffsetDateTime) body(decoded(e)).get("occurredAt");

        assertEquals(OffsetDateTime.parse("2026-11-18T10:00:00.123456Z"), at);
        assertEquals(0, at.getOffset().getTotalSeconds());
    }

    @Test
    void timesWithAndWithoutAFractionAreAccepted() throws Exception {
        for (String ok : new String[] {"2026-11-18T10:00:00Z", "2026-11-18T10:00:00.1Z", "2026-11-18T10:00:00.123456789Z", "2026-11-18T10:00:00+00:00", "2026-11-18T10:00:00.5+14:00", "2026-02-28T23:59:59Z", "2028-02-29T00:00:00Z"}) {
            Map<String, Object> e = event();
            e.put("occurred_at", ok);
            assertEquals(OffsetDateTime.parse(ok).toInstant(), ((OffsetDateTime) body(decoded(e)).get("occurredAt")).toInstant(), ok);
        }
    }

    @Test
    void aTimeThatIsNotRfc3339IsRefused() throws Exception {
        java.util.List<String> notRefused = new java.util.ArrayList<>();
        for (String bad : new String[] {"yesterday", "2026-11-18", "2026-11-18 10:00:00Z", "2026-11-18T10:00:00", "2026-13-01T00:00:00Z", "2026-02-30T00:00:00Z", "2027-02-29T00:00:00Z",
                "2026-11-18T25:00:00Z", "2026-11-18T10:61:00Z", "2026-11-18T10:00:00+25:00", "1700000000", "2026-11-18T10:00:00", " 2026-11-18T10:00:00Z", "2026-11-18T10:00:00Z "}) {
            Map<String, Object> e = event();
            e.put("occurred_at", bad);
            try {
                decoded(e);
                notRefused.add(bad);
            } catch (PoisonRecordException refusal) {
                assertEquals("occurred_at", refusal.getField().isEmpty() ? "occurred_at" : refusal.getField(), bad);
            }
        }
        assertEquals(java.util.List.of(), notRefused, "these times were accepted");
    }

    @Test
    void aLowercaseTAndZAreAcceptedBecauseRfc3339AllowsThem() throws Exception {
        // The reference (Go's time.Parse) refuses them; no producer writes them. Recorded as a known difference.
        for (String lower : new String[] {"2026-11-18T10:00:00z", "2026-11-18t10:00:00Z"}) {
            Map<String, Object> e = event();
            e.put("occurred_at", lower);
            assertEquals(OffsetDateTime.parse("2026-11-18T10:00:00Z").toInstant(), ((OffsetDateTime) body(decoded(e)).get("occurredAt")).toInstant());
        }
    }

    @Test
    void aMissingTimeIsMissingNotMalformed() throws Exception {
        Map<String, Object> e = event();
        e.remove("occurred_at");

        assertReason(refused(e), "field-missing", "occurred_at");
    }

    // ---- the documents ----------------------------------------------------------------------------------------------------

    @Test
    void aPayloadOrMetadataThatIsNotAnObjectOrArrayIsRefused() throws Exception {
        for (String bad : new String[] {"123", "\"text\"", "true", "null", "", "not json", "{", "{\"a\":}"}) {
            Map<String, Object> e = event();
            e.put("payload", bad);
            PoisonRecordException refusal = refused(e);
            assertTrue(refusal.getReason().equals("payload") || refusal.getReason().equals("field-missing"), bad + " -> " + refusal.getMessage());
        }
        Map<String, Object> badMetadata = event();
        badMetadata.put("metadata", "42");
        assertReason(refused(badMetadata), "metadata", "metadata");
    }

    @Test
    void aMissingPayloadOrMetadataIsMissing() throws Exception {
        Map<String, Object> noPayload = event();
        noPayload.remove("payload");
        Map<String, Object> noMetadata = event();
        noMetadata.remove("metadata");

        assertReason(refused(noPayload), "field-missing", "payload");
        assertReason(refused(noMetadata), "field-missing", "metadata");
    }

    @Test
    void aTwoMegabytePayloadIsDecodedWhole() throws Exception {
        Map<String, Object> e = event();
        e.put("event_name", "ToolCallSummary");
        e.put("payload", JSON.writeValueAsString(Map.of("blob", repeat("z", 2_000_000))));

        assertEquals(2_000_000 + 11, ((String) body(decoded(e)).get("payload")).length());
    }

    @Test
    void aDeeplyNestedPayloadIsNotAStackOverflowButAnAnswer() throws Exception {
        String nested = repeat("[", 500) + repeat("]", 500);
        Map<String, Object> e = event();
        e.put("event_name", "ToolCallSummary");
        e.put("payload", nested);

        Exchange exchange = decoded(e);

        assertEquals(nested, body(exchange).get("payload"));
    }

    // ---- the wire format --------------------------------------------------------------------------------------------------

    @Test
    void nothingAtAllAndNullAndBlankAreTombstones() throws Exception {
        for (byte[] value : new byte[][] {null, new byte[0], framed(2, "null".getBytes(StandardCharsets.UTF_8)), framed(2, "  \n ".getBytes(StandardCharsets.UTF_8)), framed(2, new byte[0])}) {
            Exchange exchange = exchange(value);

            decoder.decode(exchange);

            assertEquals(true, exchange.getMessage().getHeader("Tombstone"));
        }
    }

    @Test
    void aValueWithoutTheMagicByteOrWithAShortHeaderIsRefused() throws Exception {
        assertReason(assertThrows(PoisonRecordException.class, () -> decoder.decode(exchange("{\"a\":1}".getBytes(StandardCharsets.UTF_8)))), "no-schema-header", "");
        assertReason(assertThrows(PoisonRecordException.class, () -> decoder.decode(exchange(new byte[] {0, 0, 0, 2}))), "header", "");
        assertReason(assertThrows(PoisonRecordException.class, () -> decoder.decode(exchange(new byte[] {0}))), "header", "");
    }

    @Test
    void aSchemaIdThatIsNotAllowedIsRefusedBeforeTheBodyIsRead() throws Exception {
        for (int id : new int[] {0, 1, 3, 9, 2_000_000, -1}) {
            PoisonRecordException refusal = assertThrows(PoisonRecordException.class, () -> decoder.decode(exchange(framed(id, JSON.writeValueAsBytes(event())))));
            assertReason(refusal, "schema-id", "");
        }
    }

    @Test
    void invalidJsonAndNonObjectsAreRefusedAsJson() throws Exception {
        for (String bad : new String[] {"{bad json", "[1,2]", "\"x\"", "42", "true", "{\"a\":1}{\"b\":2}x"}) {
            PoisonRecordException refusal = assertThrows(PoisonRecordException.class, () -> decoder.decode(exchange(framed(2, bad.getBytes(StandardCharsets.UTF_8)))), bad);
            assertEquals("json", refusal.getReason(), bad);
        }
    }

    @Test
    void invalidUtf8IsRefusedNotACrash() {
        byte[] garbage = framed(2, new byte[] {'{', '"', 'a', '"', ':', '"', (byte) 0xff, (byte) 0xfe, '"', '}'});

        PoisonRecordException refusal = assertThrows(PoisonRecordException.class, () -> decoder.decode(exchange(garbage)));

        assertFalse(refusal.getReason().isEmpty());
    }

    @Test
    void aBomBeforeTheJsonIsRefusedAsJson() {
        byte[] withBom = framed(2, ("﻿" + "{}").getBytes(StandardCharsets.UTF_8));

        assertThrows(PoisonRecordException.class, () -> decoder.decode(exchange(withBom)));
    }

    @Test
    void theDeadLetterTextShowsTheHeaderBytesAndCutsAHugeRecord() throws Exception {
        Exchange exchange = decoded(event());
        String shown = exchange.getProperty("rawText", String.class);

        assertTrue(shown.startsWith("\\x00\\x00\\x00\\x00\\x02{"), shown.substring(0, 30));
        assertTrue(ConversationEventDecoder.printable(new byte[40_000]).length() < 70_000);
        assertTrue(ConversationEventDecoder.printable("x".repeat(40_000).getBytes(StandardCharsets.UTF_8)).contains("...(truncated, 40000 chars)"));
        assertEquals("", ConversationEventDecoder.printable(null));
    }

    @Test
    void theKeyIsShownReadableToo() throws Exception {
        Exchange exchange = exchange(framed(event()));
        exchange.getMessage().setHeader("CamelKafkaKey", framed(1, "{\"conversation_id\":\"conv-1\"}".getBytes(StandardCharsets.UTF_8)));

        decoder.decode(exchange);

        assertEquals("\\x00\\x00\\x00\\x00\\x01{\"conversation_id\":\"conv-1\"}", exchange.getProperty("rawKey"));
    }

    @Test
    void theRegistryBeingUnreachableIsNotTheRecordsFault() {
        ConversationEventDecoder lonely = new ConversationEventDecoder();
        DefaultCamelContext other = contextFor("http://127.0.0.1:1", true);
        Exchange exchange = new DefaultExchange(other);
        try {
            exchange.getMessage().setBody(framed(event()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }

        java.io.UncheckedIOException failure = assertThrows(java.io.UncheckedIOException.class, () -> lonely.decode(exchange));

        assertInstanceOf(java.io.IOException.class, failure.getCause(), "retried forever, never dead-lettered");
    }

    @Test
    void anUnframedRecordIsAcceptedWhenFramingIsNotRequired() throws Exception {
        ConversationEventDecoder plain = new ConversationEventDecoder();
        DefaultCamelContext other = contextFor("http://schema-registry:8081", false);
        Exchange exchange = new DefaultExchange(other);
        exchange.getMessage().setBody(JSON.writeValueAsBytes(event()));

        plain.decode(exchange);

        assertEquals("hola", body(exchange).get("text"));
    }
}

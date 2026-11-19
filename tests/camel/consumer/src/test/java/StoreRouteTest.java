import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;
import org.apache.camel.main.Main;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Route 2 (store) and route 3 (dead-letter) of the consumer with their real error rules, on the cluster's Postgres: what
 * each kind of event leaves in the three tables, what the store refuses for good, and what concurrency does. The decoder is
 * the project's own bean, so an event goes through it as route 1 would. Rows are keyed "jt-" and removed after each test.
 */
class StoreRouteTest {

    private static final ObjectMapper JSON = new ObjectMapper();
    private static HikariDataSource db;
    private static Main main;
    private static ProducerTemplate template;
    private static ConversationEventDecoder decoder;
    private final AtomicInteger commits = new AtomicInteger();
    /** How many dead-letter rows of this service existed at the moment of the commit: the offset must move only after the row is written. */
    private volatile String rowsAtCommit;
    private final AtomicInteger offset = new AtomicInteger();
    private byte[] lastValue;

    @BeforeAll
    static void boot() throws Exception {
        db = new HikariDataSource();
        db.setJdbcUrl(System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://db:5432/postgres?currentSchema=history"));
        db.setUsername("postgres");
        db.setMaximumPoolSize(16);
        decoder = new ConversationEventDecoder();
        main = new Main();
        main.bind("pgDataSource", db);
        main.bind("storeOutcome", new StoreOutcome());
        main.bind("storeRefusal", new StoreRefusal());
        main.bind("decoder", decoder);
        main.addInitialProperty("app.schema.registryUrl", System.getenv().getOrDefault("REGISTRY_URL", "http://schema-registry:8081"));
        main.addInitialProperty("app.schema.allowedIds", "2");
        main.addInitialProperty("app.kafka.groupId", "jt-service");
        main.addInitialProperty("app.unexpected.maxRetries", "3");
        main.addInitialProperty("app.schema.requireFraming", "true");
        // The shared Kamelets, as Karavan copies them into a run.
        main.addInitialProperty("camel.component.kamelet.location", "file:../../../kamelets");
        main.configure().withRoutesIncludePattern("file:../../../ms-ml-conv-history-consumer/store-conversation-history.camel.yaml,file:../../../ms-ml-conv-history-consumer/handle-errors-and-dead-letters.camel.yaml");
        main.start();
        template = main.getCamelContext().createProducerTemplate();
    }

    @AfterAll
    static void shutDown() throws Exception {
        main.stop();
        clean();
        db.close();
    }

    @BeforeEach
    @AfterEach
    void cleanRows() throws Exception {
        clean();
    }

    private static void clean() throws Exception {
        sql("DELETE FROM conversation_inbox WHERE conversation_id LIKE 'jt-%'");
        sql("DELETE FROM conversation_messages WHERE conversation_id LIKE 'jt-%'");
        sql("DELETE FROM conversation_summaries WHERE conversation_id LIKE 'jt-%'");
        sql("DELETE FROM dead_letter.dead_letters WHERE service = 'jt-service'");
    }

    private static void sql(String statement) throws Exception {
        try (Connection c = db.getConnection(); Statement s = c.createStatement()) {
            s.execute(statement);
        }
    }

    private static String one(String query) throws Exception {
        try (Connection c = db.getConnection(); Statement s = c.createStatement(); ResultSet r = s.executeQuery(query)) {
            return r.next() ? r.getString(1) : null;
        }
    }

    // ---- events -----------------------------------------------------------------------------------------------------------

    private static Map<String, Object> event(String conversation, int sequence, String name, String payload) {
        Map<String, Object> e = new LinkedHashMap<>();
        e.put("conversation_id", conversation);
        e.put("sequence", sequence);
        e.put("event_name", name);
        e.put("event_version", 1);
        e.put("payload", payload);
        e.put("metadata", "{}");
        e.put("correlation_id", "corr");
        e.put("id_yape_account", "jt-acct");
        e.put("actor", "user");
        e.put("flow", "p2p");
        e.put("occurred_at", String.format("2026-11-18T10:%02d:%02d.000000Z", sequence / 60 % 60, sequence % 60));
        return e;
    }

    private static Map<String, Object> user(String conversation, int sequence, String text) throws Exception {
        return event(conversation, sequence, "UserMessageSent", JSON.writeValueAsString(Map.of("text", text)));
    }

    /** What the Kafka endpoint and route 1 do: decode, then store. Returns the exchange after the routes have run. */
    private Exchange receive(Map<String, Object> event) throws Exception {
        byte[] json = JSON.writeValueAsBytes(event);
        byte[] value = new byte[5 + json.length];
        ByteBuffer.wrap(value).putInt(1, 2);
        System.arraycopy(json, 0, value, 5, json.length);
        lastValue = value;
        Exchange exchange = main.getCamelContext().getEndpoint("direct:store").createExchange();
        exchange.getMessage().setBody(value);
        // What the kafka-at-least-once-source Kamelet does before it calls the route: keep the original value.
        exchange.setProperty("originalValue", value);
        exchange.getMessage().setHeader("CamelKafkaTopic", "jt-topic");
        exchange.getMessage().setHeader("CamelKafkaPartition", 0);
        exchange.getMessage().setHeader("CamelKafkaOffset", (long) offset.incrementAndGet());
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {KafkaManualCommit.class},
                (proxy, method, args) -> {
                    // Camel calls hashCode() on the header's object before it calls commit(); a proxy that answers null to it fails with an NPE.
                    switch (method.getName()) {
                        case "commit" -> {
                            commits.incrementAndGet();
                            rowsAtCommit = one("SELECT count(*) FROM dead_letter.dead_letters WHERE service = 'jt-service'");
                            return null;
                        }
                        case "hashCode" -> {
                            return System.identityHashCode(proxy);
                        }
                        case "equals" -> {
                            return proxy == args[0];
                        }
                        case "toString" -> {
                            return "KafkaManualCommit(test)";
                        }
                        default -> {
                            return null;
                        }
                    }
                }));
        decoder.decode(exchange);
        return template.send("direct:store", exchange);
    }

    private int count(String table, String conversation) throws Exception {
        return Integer.parseInt(one("SELECT count(*) FROM " + table + " WHERE conversation_id = '" + conversation + "'"));
    }

    // ---- what each kind of event leaves -----------------------------------------------------------------------------------

    @Test
    void aChatEventLeavesAnInboxRowAMessageAndASummary() throws Exception {
        Exchange done = receive(user("jt-1", 1, "hola"));

        assertEquals("stored", done.getMessage().getHeader("outcome"));
        assertEquals(true, done.getMessage().getHeader("projected"));
        assertEquals(1, count("conversation_inbox", "jt-1"));
        assertEquals("hola|appUser|jt-acct", one("SELECT text || '|' || role || '|' || author_id FROM conversation_messages WHERE conversation_id = 'jt-1'"));
        assertEquals("1", one("SELECT message_count FROM conversation_summaries WHERE conversation_id = 'jt-1'"));
        assertEquals(0, commits.get(), "route 2 does not commit: the Kamelet source does, when the route returns");
    }

    @Test
    void aRedeliveryWritesNothingAndSaysDuplicate() throws Exception {
        receive(user("jt-2", 1, "hola"));

        Exchange again = receive(user("jt-2", 1, "hola"));

        assertEquals("duplicate", again.getMessage().getHeader("outcome"));
        assertEquals(1, count("conversation_inbox", "jt-2"));
        assertEquals("1", one("SELECT message_count FROM conversation_summaries WHERE conversation_id = 'jt-2'"), "a redelivery does not count twice");
    }

    @Test
    void aDifferentEventAtATakenPositionIsRefusedForGoodAndTheOldOneStays() throws Exception {
        receive(user("jt-3", 1, "original"));

        Exchange clash = receive(user("jt-3", 1, "someone else"));

        assertEquals("store-rejected", clash.getProperty("dlqReason"));
        assertEquals("stream-position", clash.getProperty("dlqField"));
        assertEquals(1, commits.get(), "route 3 commits: a handled error stops the route that called route 1, so the Kamelet's own commit never runs for a dead letter");
        assertEquals("original", one("SELECT text FROM conversation_messages WHERE conversation_id = 'jt-3'"));
    }

    @Test
    void anEventThatIsNotChatIsInTheInboxOnly() throws Exception {
        Exchange done = receive(event("jt-4", 1, "ToolCallSummary", "{\"tool\":\"lookup\"}"));

        assertEquals("stored", done.getMessage().getHeader("outcome"));
        assertEquals(false, done.getMessage().getHeader("projected"));
        assertEquals(1, count("conversation_inbox", "jt-4"));
        assertEquals(0, count("conversation_messages", "jt-4"));
        assertEquals(0, count("conversation_summaries", "jt-4"));
    }

    @Test
    void aUserMessageWithNoAccountIsInTheInboxButNotAMessage() throws Exception {
        Map<String, Object> e = user("jt-5", 1, "orphan");
        e.remove("id_yape_account");

        Exchange done = receive(e);

        assertEquals(true, done.getMessage().getHeader("invalid"));
        assertEquals(1, count("conversation_inbox", "jt-5"));
        assertEquals(0, count("conversation_messages", "jt-5"));
    }

    @Test
    void anAgentMessageWithNoAccountIsAMessageWithoutASummary() throws Exception {
        Map<String, Object> e = event("jt-6", 1, "AgentMessageSent", "{\"text\":\"hello\"}");
        e.remove("id_yape_account");
        e.put("actor", "agent");

        receive(e);

        assertEquals(1, count("conversation_messages", "jt-6"));
        assertEquals(0, count("conversation_summaries", "jt-6"));
    }

    // ---- the summary ------------------------------------------------------------------------------------------------------

    @Test
    void eventsOutOfOrderGiveTheRightCountAndTheEarliestAndLatestTimes() throws Exception {
        for (int seq : new int[] {3, 1, 2}) {
            receive(user("jt-7", seq, "m" + seq));
        }

        assertEquals("3", one("SELECT message_count FROM conversation_summaries WHERE conversation_id = 'jt-7'"));
        assertEquals("2026-11-18 10:00:01+00", one("SELECT first_occurred_at FROM conversation_summaries WHERE conversation_id = 'jt-7'").replace(" UTC", "+00"));
        assertEquals("2026-11-18 10:00:03+00", one("SELECT last_occurred_at FROM conversation_summaries WHERE conversation_id = 'jt-7'").replace(" UTC", "+00"));
    }

    @Test
    void aMessageRowThatAlreadyExistsWithoutItsInboxRowIsUpdatedAndNotCountedAgain() throws Exception {
        receive(user("jt-8", 1, "first"));
        sql("DELETE FROM conversation_inbox WHERE conversation_id = 'jt-8'");   // the inbox lost the row; the message stayed

        Exchange again = receive(user("jt-8", 1, "first"));

        assertEquals("stored", again.getMessage().getHeader("outcome"));
        assertEquals("1", one("SELECT message_count FROM conversation_summaries WHERE conversation_id = 'jt-8'"), "the summary counts a message once");
        assertEquals(1, count("conversation_messages", "jt-8"));
    }

    // ---- what the store cannot hold ---------------------------------------------------------------------------------------

    @Test
    void aTextWithNulIsRefusedForGoodNotRetriedForeverAndNothingIsHalfWritten() throws Exception {
        Exchange refused = receive(user("jt-9", 1, "before\u0000after"));

        assertEquals("store-rejected", refused.getProperty("dlqReason"));
        assertTrue(String.valueOf(refused.getProperty("dlqField")).startsWith("sqlstate-22"), String.valueOf(refused.getProperty("dlqField")));
        assertEquals(1, commits.get(), "dead-lettered and committed, so a refused message cannot block its partition (exception on the exchange: " + refused.getException() + ")");
        assertEquals("1", rowsAtCommit, "the offset moves only after the dead-letter row is written");
        assertEquals(0, count("conversation_inbox", "jt-9"));
        assertEquals(0, count("conversation_messages", "jt-9"));
        assertEquals(0, count("conversation_summaries", "jt-9"));
    }

    @Test
    void aNulInAnythingOfThePayloadIsRefusedTheSameWay() throws Exception {
        Exchange refused = receive(event("jt-10", 1, "ToolCallSummary", "{\"tool\":\"a\\u0000b\"}"));

        assertEquals("store-rejected", refused.getProperty("dlqReason"));
        assertEquals(1, commits.get(), "dead-lettered and committed");
        assertEquals(0, count("conversation_inbox", "jt-10"));
    }

    @Test
    void aLoneSurrogateInTheTextIsStoredAsAQuestionMarkAndDoesNotStopTheStream() throws Exception {
        // A half of an emoji cannot be UTF-8: the JDBC driver encodes it as '?'. Recorded as a known limit, not a failure.
        Exchange done = receive(user("jt-11", 1, "a\uD800b"));

        assertEquals("stored", done.getMessage().getHeader("outcome"));
        assertEquals("a?b", one("SELECT text FROM conversation_messages WHERE conversation_id = 'jt-11'"));
    }

    // ---- text that is hard to carry, and big ------------------------------------------------------------------------------

    @Test
    void awkwardTextRoundTripsThroughThePostgresColumn() throws Exception {
        String[] texts = {"👨‍👩‍👧 👍🏽", "é é", "שלום مرحبا 中文", "tab\tnew\nline\r\n", "  ​﻿",
                "'; DROP TABLE conversation_messages;--", "\\\\ \"quoted\" {\"a\":1}", "  spaced  ", ""};
        for (int i = 0; i < texts.length; i++) {
            receive(user("jt-12", i + 1, texts[i]));
        }

        for (int i = 0; i < texts.length; i++) {
            assertEquals(texts[i], one("SELECT text FROM conversation_messages WHERE conversation_id = 'jt-12' AND sequence = " + (i + 1)), "text #" + i);
        }
    }

    @Test
    void aMillionCharacterTextIsStoredWhole() throws Exception {
        String big = "x".repeat(1_000_000);

        receive(user("jt-13", 1, big));

        assertEquals("1000000", one("SELECT length(text) FROM conversation_messages WHERE conversation_id = 'jt-13'"));
    }

    // ---- concurrency ------------------------------------------------------------------------------------------------------

    @Test
    void sixteenThreadsDeliveringTheSameEventStoreItExactlyOnce() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(16);
        List<CompletableFuture<String>> all = new ArrayList<>();
        Map<String, Object> e = user("jt-14", 1, "same");
        for (int i = 0; i < 16; i++) {
            all.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return String.valueOf(receive(e).getMessage().getHeader("outcome"));
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }, pool));
        }
        int stored = 0;
        for (CompletableFuture<String> f : all) {
            if (f.get().equals("stored")) {
                stored++;
            }
        }
        pool.shutdown();

        assertEquals(1, stored, "one winner, the rest are duplicates");
        assertEquals(1, count("conversation_inbox", "jt-14"));
        assertEquals("1", one("SELECT message_count FROM conversation_summaries WHERE conversation_id = 'jt-14'"));
    }

    @Test
    void thirtyThreadsWritingDifferentMessagesOfOneConversationCountThemAll() throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(15);
        List<CompletableFuture<Exchange>> all = new ArrayList<>();
        for (int i = 1; i <= 30; i++) {
            Map<String, Object> e = user("jt-15", i, "m" + i);
            all.add(CompletableFuture.supplyAsync(() -> {
                try {
                    return receive(e);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }, pool));
        }
        for (CompletableFuture<Exchange> f : all) {
            assertEquals("stored", f.get().getMessage().getHeader("outcome"));
        }
        pool.shutdown();

        assertEquals("30", one("SELECT message_count FROM conversation_summaries WHERE conversation_id = 'jt-15'"), "no lost update on the summary");
        assertEquals(30, count("conversation_messages", "jt-15"));
        assertNull(one("SELECT 1 FROM conversation_summaries WHERE conversation_id = 'jt-15' AND first_occurred_at > last_occurred_at"));
    }

    // ---- the dead-letter table ----------------------------------------------------------------------------------------------

    private static String hex(byte[] bytes) {
        return java.util.HexFormat.of().formatHex(bytes);
    }

    /** What route 1's error rules do: decode, and when the record is refused (or fails), call route 3. Returns the exchange after it. */
    private Exchange deadLetter(byte[] key, byte[] value, int partition, long recordOffset, Throwable caught, int redeliveries) {
        Exchange exchange = main.getCamelContext().getEndpoint("direct:deadLetter").createExchange();
        exchange.getMessage().setBody(value);
        exchange.setProperty("originalValue", value);
        exchange.getMessage().setHeader("CamelKafkaTopic", "jt-topic");
        exchange.getMessage().setHeader("CamelKafkaPartition", partition);
        exchange.getMessage().setHeader("CamelKafkaOffset", recordOffset);
        exchange.getMessage().setHeader("CamelKafkaTimestamp", 1_760_000_000_000L);
        if (key != null) {
            exchange.getMessage().setHeader("CamelKafkaKey", key);
        }
        exchange.getMessage().setHeader("source", "agent".getBytes(java.nio.charset.StandardCharsets.UTF_8));
        try {
            decoder.decode(exchange);
        } catch (PoisonRecordException refused) {
            exchange.setProperty(Exchange.EXCEPTION_CAUGHT, refused);
        }
        if (caught != null) {
            exchange.setProperty(Exchange.EXCEPTION_CAUGHT, caught);
        }
        if (redeliveries > 0) {
            exchange.getMessage().setHeader(Exchange.REDELIVERY_COUNTER, redeliveries);
        }
        return template.send("direct:deadLetter", exchange);
    }

    private static String deadLetter(String column, long recordOffset) throws Exception {
        return one("SELECT " + column + " FROM dead_letter.dead_letters WHERE service = 'jt-service' AND source_offset = " + recordOffset);
    }

    @Test
    void aRefusedRecordIsStoredExactlyAsItCameWithWhereItWas() throws Exception {
        byte[] key = {0, 0, 0, 0, 1, '{', (byte) 0xff, 0, '}'};
        byte[] value = {0, 0, 0, 0, 2, '{', (byte) 0xff, 0, '}'};   // a NUL and a byte that is not UTF-8: refused, and must come back as is

        Exchange done = deadLetter(key, value, 3, 7001L, null, 0);

        assertNull(done.getException(), "writing the dead letter must not fail");
        assertEquals(hex(value), deadLetter("encode(record_value, 'hex')", 7001));
        assertEquals(hex(key), deadLetter("encode(record_key, 'hex')", 7001));
        assertEquals("jt-topic", deadLetter("source_topic", 7001));
        assertEquals("3", deadLetter("source_partition", 7001));
        assertEquals(String.valueOf(done.getProperty("dlqReason")), deadLetter("reason", 7001));
        assertEquals("1", deadLetter("attempt", 7001));
        assertEquals("open", deadLetter("status", 7001));
        assertEquals("agent", deadLetter("headers ->> 'source'", 7001));
        assertEquals("1760000000000", deadLetter("(extract(epoch from source_timestamp) * 1000)::bigint", 7001));
    }

    @Test
    void theSameRecordTwiceLeavesOneRow() throws Exception {
        byte[] value = {0, 0, 0, 0, 2, '{', '}'};

        deadLetter(null, value, 0, 7002L, null, 0);
        Exchange again = deadLetter(null, value, 0, 7002L, null, 0);

        assertNull(again.getException(), "a retry after the insert succeeded must not fail");
        assertEquals("1", one("SELECT count(*) FROM dead_letter.dead_letters WHERE service = 'jt-service' AND source_offset = 7002"));
        assertNull(deadLetter("record_key", 7002), "a record with no key has a NULL key");
    }

    @Test
    void aRecordTheStoreRefusesKeepsTheOriginalBytesNotTheDecodedEvent() throws Exception {
        receive(user("jt-nul-dl", 1, "before\u0000after"));

        String reason = one("SELECT reason FROM dead_letter.dead_letters WHERE service = 'jt-service'");
        assertEquals("store-rejected", reason);
        assertTrue(one("SELECT field FROM dead_letter.dead_letters WHERE service = 'jt-service'").startsWith("sqlstate-22"));
        assertEquals(hex(lastValue), one("SELECT encode(record_value, 'hex') FROM dead_letter.dead_letters WHERE service = 'jt-service'"),
                "the row holds what Kafka delivered, not the event the decoder built from it");
        assertEquals("1", one("SELECT count(*) FROM dead_letter.dead_letters WHERE service = 'jt-service'"));
    }

    @Test
    void anUnexpectedErrorRecordsItsClassItsAttemptsAndAMessageWithoutANul() throws Exception {
        byte[] value = framedValid("jt-unexpected-dl");

        Exchange done = deadLetter(null, value, 1, 7003L, new IllegalStateException("boom \u0000 now"), 3);

        assertNull(done.getException());
        assertEquals("unexpected", deadLetter("reason", 7003));
        assertEquals("IllegalStateException", deadLetter("field", 7003));
        assertEquals("boom  now", deadLetter("detail", 7003));
        assertEquals("java.lang.IllegalStateException", deadLetter("exception_class", 7003));
        assertEquals("4", deadLetter("attempt", 7003), "app.unexpected.maxRetries is 3: the record was tried 4 times");
        assertEquals(hex(value), deadLetter("encode(record_value, 'hex')", 7003), "a valid record that failed is also kept as it came");
    }

    /** A valid framed record, as the Kafka endpoint would hand it over. */
    private byte[] framedValid(String conversation) throws Exception {
        byte[] json = JSON.writeValueAsBytes(user(conversation, 1, "hola"));
        byte[] value = new byte[5 + json.length];
        ByteBuffer.wrap(value).putInt(1, 2);
        System.arraycopy(json, 0, value, 5, json.length);
        return value;
    }
}

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
    private final AtomicInteger offset = new AtomicInteger();

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
        main.addInitialProperty("app.schema.requireFraming", "true");
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
        Exchange exchange = main.getCamelContext().getEndpoint("direct:store").createExchange();
        exchange.getMessage().setBody(value);
        exchange.getMessage().setHeader("CamelKafkaTopic", "jt-topic");
        exchange.getMessage().setHeader("CamelKafkaPartition", 0);
        exchange.getMessage().setHeader("CamelKafkaOffset", (long) offset.incrementAndGet());
        exchange.getMessage().setHeader(KafkaConstants.MANUAL_COMMIT, Proxy.newProxyInstance(getClass().getClassLoader(), new Class<?>[] {KafkaManualCommit.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("commit")) {
                        commits.incrementAndGet();
                    }
                    return null;
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
        assertEquals(0, commits.get(), "route 3 does not commit either: the Kamelet source does, when the route returns (test-consumer.sh checks there is no lag)");
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
        assertEquals(0, commits.get(), "dead-lettered, and no route commits: the Kamelet source does, so a refused message cannot block its partition");
        assertEquals(0, count("conversation_inbox", "jt-9"));
        assertEquals(0, count("conversation_messages", "jt-9"));
        assertEquals(0, count("conversation_summaries", "jt-9"));
    }

    @Test
    void aNulInAnythingOfThePayloadIsRefusedTheSameWay() throws Exception {
        Exchange refused = receive(event("jt-10", 1, "ToolCallSummary", "{\"tool\":\"a\\u0000b\"}"));

        assertEquals("store-rejected", refused.getProperty("dlqReason"));
        assertEquals(0, commits.get(), "no route commits");
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
}

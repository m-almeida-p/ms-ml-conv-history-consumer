import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.zaxxer.hikari.HikariDataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HexFormat;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.camel.Exchange;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.main.Main;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The shared dead-letter-sink Kamelet on the cluster's Postgres, with no decoder and no store in the way: what row it builds from
 * the exchange it is given. Route 3 of the consumer is only the call to it, so this is where the edge cases of the row live
 * (StoreRouteTest covers the same Kamelet behind the real error rules). Rows belong to the service "jt-sink" and are removed.
 */
class DeadLetterSinkTest {

    private static HikariDataSource db;
    private static Main main;
    private static ProducerTemplate template;
    private static final AtomicLong OFFSET = new AtomicLong(9000);

    @BeforeAll
    static void boot() throws Exception {
        db = new HikariDataSource();
        db.setJdbcUrl(System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://db:5432/postgres?currentSchema=history"));
        db.setUsername("postgres");
        db.setMaximumPoolSize(4);
        main = new Main();
        main.bind("pgDataSource", db);
        main.bind("storeRefusal", new StoreRefusal());
        main.addInitialProperty("app.kafka.groupId", "jt-sink");
        main.addInitialProperty("app.unexpected.maxRetries", "5");
        main.addInitialProperty("camel.component.kamelet.location", "file:../../../kamelets");
        main.configure().withRoutesIncludePattern("file:../../../ms-ml-conv-history-consumer/handle-errors-and-dead-letters.camel.yaml");
        main.start();
        template = main.getCamelContext().createProducerTemplate();
    }

    @AfterAll
    static void shutDown() throws Exception {
        main.stop();
        update("DELETE FROM dead_letter.dead_letters WHERE service = 'jt-sink'");
        db.close();
    }

    @BeforeEach
    void clean() throws Exception {
        update("DELETE FROM dead_letter.dead_letters WHERE service = 'jt-sink'");
    }

    private static void update(String sql) throws Exception {
        try (Connection c = db.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate(sql);
        }
    }

    private static String one(String sql) throws Exception {
        try (Connection c = db.getConnection(); Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            return r.next() ? r.getString(1) : null;
        }
    }

    /** The exchange the consumer's error rules hand to route 3: the record's Kafka coordinates, its original value, and the reason. */
    private Exchange dead(byte[] value) {
        Exchange exchange = main.getCamelContext().getEndpoint("direct:deadLetter").createExchange();
        exchange.getMessage().setBody("decoded, not the record");
        exchange.setProperty("originalValue", value);
        exchange.getMessage().setHeader("CamelKafkaTopic", "jt-topic");
        exchange.getMessage().setHeader("CamelKafkaPartition", 0);
        exchange.getMessage().setHeader("CamelKafkaOffset", OFFSET.incrementAndGet());
        return exchange;
    }

    private String column(Exchange sent, String column) throws Exception {
        long recordOffset = sent.getMessage().getHeader("CamelKafkaOffset", Long.class);
        return one("SELECT " + column + " FROM dead_letter.dead_letters WHERE service = 'jt-sink' AND source_offset = " + recordOffset);
    }

    private Exchange send(Exchange exchange) {
        Exchange done = template.send("direct:deadLetter", exchange);
        assertNull(done.getException(), String.valueOf(done.getException()));
        return done;
    }

    private static String hex(byte[] bytes) {
        return HexFormat.of().formatHex(bytes);
    }

    @Test
    void theOriginalBytesAreStoredWhateverTheyHold() throws Exception {
        byte[] value = {0, 0, 0, 0, 2, '{', (byte) 0xff, 0, '}'};
        Exchange exchange = dead(value);
        exchange.setProperty("dlqReason", "json");

        send(exchange);

        assertEquals(hex(value), column(exchange, "encode(record_value, 'hex')"), "a NUL and bytes that are not UTF-8 are kept as they came");
    }

    @Test
    void aTextKeyIsItsUtf8BytesAndNoKeyIsNull() throws Exception {
        Exchange withKey = dead(new byte[] {1});
        withKey.setProperty("dlqReason", "json");
        withKey.getMessage().setHeader("CamelKafkaKey", "clave-é");
        send(withKey);
        Exchange withoutKey = dead(new byte[] {1});
        withoutKey.setProperty("dlqReason", "json");
        send(withoutKey);

        assertEquals(hex("clave-é".getBytes(StandardCharsets.UTF_8)), column(withKey, "encode(record_key, 'hex')"));
        assertNull(column(withoutKey, "record_key"));
    }

    @Test
    void theKafkaHeadersAreKeptAsJsonWithoutCamelsOwnNorTheOnesARouteAdded() throws Exception {
        Exchange exchange = dead(new byte[] {1});
        exchange.setProperty("dlqReason", "json");
        exchange.getMessage().setHeader("source", "agent".getBytes(StandardCharsets.UTF_8));
        exchange.getMessage().setHeader("binary", new byte[] {(byte) 0xff, 0x01});
        exchange.getMessage().setHeader("withnul", new byte[] {'a', 0, 'b'});
        exchange.getMessage().setHeader("CamelSomething", "internal".getBytes(StandardCharsets.UTF_8));
        exchange.getMessage().setHeader("outcome", "stored");
        exchange.getMessage().setHeader("Tombstone", false);

        send(exchange);

        assertEquals("agent", column(exchange, "headers ->> 'source'"));
        assertEquals("base64:/wE=", column(exchange, "headers ->> 'binary'"));
        assertEquals("base64:YQBi", column(exchange, "headers ->> 'withnul'"), "a NUL cannot go in jsonb, so the header is kept as base64");
        assertEquals("3", column(exchange, "(SELECT count(*) FROM jsonb_object_keys(headers))"), "Camel's own headers and the ones a route added are not the record's");
    }

    @Test
    void aNulInATextIsRemovedAndAHugeDetailIsCut() throws Exception {
        Exchange exchange = dead(new byte[] {1});
        exchange.setProperty("dlqReason", "re\u0000ason");
        exchange.setProperty("dlqField", "fi\u0000eld");
        exchange.setProperty("dlqDetail", "bad \u0000 value " + "x".repeat(50_000));

        send(exchange);

        assertEquals("reason", column(exchange, "reason"));
        assertEquals("field", column(exchange, "field"));
        assertEquals("2000", column(exchange, "length(detail)"));
        assertTrue(column(exchange, "detail").startsWith("bad  value x"));
    }

    @Test
    void aRecordWithNoReasonIsAnUnexpectedErrorNamedAfterItsExceptionWithTheRetriesPlusOneAsItsAttempt() throws Exception {
        Exchange exchange = dead(new byte[] {1});
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("boom"));

        send(exchange);

        assertEquals("unexpected", column(exchange, "reason"));
        assertEquals("IllegalStateException", column(exchange, "field"));
        assertEquals("boom", column(exchange, "detail"));
        assertEquals("java.lang.IllegalStateException", column(exchange, "exception_class"));
        assertEquals("6", column(exchange, "attempt"), "maxRetries is 5 here: tried 6 times");
    }

    @Test
    void aRefusedRecordIsAttemptOneAndKeepsNoExceptionClass() throws Exception {
        Exchange exchange = dead(new byte[] {1});
        exchange.setProperty("dlqReason", "json");
        exchange.setProperty("dlqField", "payload");
        exchange.setProperty("dlqDetail", "not valid JSON");
        exchange.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("boom"));

        send(exchange);

        assertEquals("1", column(exchange, "attempt"));
        assertNull(column(exchange, "exception_class"), "for a refused record the class would always be the same and says nothing");
        assertEquals("payload", column(exchange, "field"));
    }

    @Test
    void whatIsMissingBecomesNullAndTheRowIsStillWritten() throws Exception {
        Exchange exchange = main.getCamelContext().getEndpoint("direct:deadLetter").createExchange();
        exchange.getMessage().setHeader("CamelKafkaTopic", "jt-topic");
        exchange.getMessage().setHeader("CamelKafkaPartition", 0);
        exchange.getMessage().setHeader("CamelKafkaOffset", OFFSET.incrementAndGet());

        send(exchange);

        assertNull(column(exchange, "record_value"));
        assertNull(column(exchange, "record_key"));
        assertNull(column(exchange, "source_timestamp"));
        assertEquals("{}", column(exchange, "headers::text"));
        assertEquals("unexpected", column(exchange, "reason"), "a record with no reason is an unexpected one");
    }

    @Test
    void theKeyForTheLogIsReadableWithTheHeaderBytesShownAndAHugeKeyIsCut() throws Exception {
        Exchange framed = dead(new byte[] {1});
        framed.setProperty("dlqReason", "json");
        byte[] json = "{\"conversation_id\":\"conv-1\"}".getBytes(StandardCharsets.UTF_8);
        byte[] key = new byte[5 + json.length];
        key[4] = 1;
        System.arraycopy(json, 0, key, 5, json.length);
        framed.getMessage().setHeader("CamelKafkaKey", key);
        Exchange huge = dead(new byte[] {1});
        huge.setProperty("dlqReason", "json");
        huge.getMessage().setHeader("CamelKafkaKey", "x".repeat(40_000).getBytes(StandardCharsets.UTF_8));

        Exchange done = send(framed);
        Exchange doneHuge = send(huge);

        assertEquals("\\x00\\x00\\x00\\x00\\x01{\"conversation_id\":\"conv-1\"}", done.getProperty("dlqKeyText", String.class));
        assertTrue(doneHuge.getProperty("dlqKeyText", String.class).contains("...(truncated, 40000 chars)"));
        assertTrue(doneHuge.getProperty("dlqKeyText", String.class).length() < 17_000);
    }
}

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Timestamp;
import java.util.Map;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.support.DefaultExchange;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** What goes into the dead-letter row: no database, only the parameters DeadLetterRecord builds from the exchange. */
class DeadLetterRecordTest {

    private final DeadLetterRecord bean = new DeadLetterRecord();
    private DefaultCamelContext context;

    @BeforeEach
    void setUp() {
        context = new DefaultCamelContext();
        java.util.Properties p = new java.util.Properties();
        p.setProperty("app.kafka.groupId", "a-group");
        p.setProperty("app.unexpected.maxRetries", "3");
        context.getPropertiesComponent().setOverrideProperties(p);
    }

    private Exchange exchange() {
        Exchange exchange = new DefaultExchange(context);
        exchange.getMessage().setHeader("CamelKafkaTopic", "a-topic");
        exchange.getMessage().setHeader("CamelKafkaPartition", 2);
        exchange.getMessage().setHeader("CamelKafkaOffset", 41L);
        return exchange;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> row(Exchange exchange) throws Exception {
        bean.prepare(exchange);
        return (Map<String, Object>) exchange.getMessage().getBody();
    }

    @Test
    void theRowCarriesWhoWhereAndWhat() throws Exception {
        Exchange exchange = exchange();
        exchange.getMessage().setHeader("CamelKafkaTimestamp", 1_760_000_000_000L);
        byte[] key = {0, 0, 0, 0, 1, '{', '}'};
        byte[] value = {0, 0, 0, 0, 2, (byte) 0xff, 0};
        exchange.setProperty("rawKeyBytes", key);
        exchange.setProperty("rawValueBytes", value);
        exchange.setProperty("rawHeaders", "{\"h\":\"v\"}");
        exchange.setProperty("dlqReason", "json");
        exchange.setProperty("dlqField", "payload");
        exchange.setProperty("dlqDetail", "not valid JSON");

        Map<String, Object> row = row(exchange);

        assertEquals("a-group", row.get("service"));
        assertEquals("a-topic", row.get("topic"));
        assertEquals(2, row.get("partition"));
        assertEquals(41L, row.get("offset"));
        assertEquals(new Timestamp(1_760_000_000_000L), row.get("timestamp"));
        assertArrayEquals(key, (byte[]) row.get("key"));
        assertArrayEquals(value, (byte[]) row.get("value"), "the record is kept byte for byte, NUL and invalid UTF-8 included");
        assertEquals("{\"h\":\"v\"}", row.get("headers"));
        assertEquals("json", row.get("reason"));
        assertEquals("payload", row.get("field"));
        assertEquals("not valid JSON", row.get("detail"));
        assertEquals(1, row.get("attempt"));
    }

    @Test
    void aTextWithANulIsCleanedSoTheInsertCannotFailOnIt() throws Exception {
        Exchange exchange = exchange();
        exchange.setProperty("dlqReason", "re\u0000ason");
        exchange.setProperty("dlqField", "fi\u0000eld");
        exchange.setProperty("dlqDetail", "bad \u0000 value");

        Map<String, Object> row = row(exchange);

        assertEquals("reason", row.get("reason"));
        assertEquals("field", row.get("field"));
        assertEquals("bad  value", row.get("detail"));
    }

    @Test
    void aHugeDetailIsCut() throws Exception {
        Exchange exchange = exchange();
        exchange.setProperty("dlqReason", "unexpected");
        exchange.setProperty("dlqDetail", "x".repeat(50_000));

        assertEquals(DeadLetterRecord.MAX_TEXT, ((String) row(exchange).get("detail")).length());
    }

    @Test
    void aRefusedRecordIsAttemptOneAndAnUnexpectedErrorIsTheRetriesPlusOne() throws Exception {
        Exchange refused = exchange();
        refused.setProperty("dlqReason", "json");
        assertEquals(1, row(refused).get("attempt"), "a poison record is not retried");

        Exchange unexpected = exchange();
        unexpected.setProperty("dlqReason", "unexpected");
        assertEquals(4, row(unexpected).get("attempt"), "app.unexpected.maxRetries is 3: the record was tried 4 times");
    }

    @Test
    void theExceptionClassIsKeptOnlyForAnUnexpectedError() throws Exception {
        Exchange unexpected = exchange();
        unexpected.setProperty("dlqReason", "unexpected");
        unexpected.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("boom"));
        assertEquals("java.lang.IllegalStateException", row(unexpected).get("exceptionClass"));

        Exchange poison = exchange();
        poison.setProperty("dlqReason", "json");
        poison.setProperty(Exchange.EXCEPTION_CAUGHT, new IllegalStateException("boom"));
        assertNull(row(poison).get("exceptionClass"), "for a poison record the class says nothing");
    }

    @Test
    void whatIsMissingBecomesNullAndTheRowStillWorks() throws Exception {
        Map<String, Object> row = row(exchange());

        assertNull(row.get("key"));
        assertNull(row.get("value"));
        assertNull(row.get("timestamp"));
        assertNull(row.get("field"));
        assertNull(row.get("detail"));
        assertEquals("{}", row.get("headers"));
        assertEquals("unexpected", row.get("reason"), "a record with no reason is an unexpected one");
        assertTrue(row.containsKey("exceptionClass"), "every parameter of the INSERT is present, even when null");
    }
}

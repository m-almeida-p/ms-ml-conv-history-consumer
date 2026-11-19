import java.sql.Timestamp;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;

/**
 * Builds the parameters of the row that route 3 writes to dead_letter.dead_letters (ms-ml-conv-history-migrations), and puts them
 * in the message body for the SQL step. The names are the ones the INSERT uses (:#service, :#topic, ...).
 *
 * What it takes from the exchange: the original key, value and headers that the decoder kept before anything changed the body, the
 * Kafka coordinates of the record, the reason, the field and the detail that the error rules or the decoder named, and how many
 * times the record was tried (from app.unexpected.maxRetries for an unexpected error, 1 otherwise).
 *
 * The text is made safe for the row: a NUL is removed (a text column refuses it, and the dead-letter insert must not fail on the very
 * thing that made the record a dead letter) and the detail is cut, so the row is always accepted. The record itself is bytes and is
 * stored as it came.
 */
@BindToRegistry("deadLetterRecord")
public class DeadLetterRecord {

    static final int MAX_TEXT = 2000;

    public void prepare(Exchange exchange) throws Exception {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("service", exchange.getContext().resolvePropertyPlaceholders("{{app.kafka.groupId}}"));
        row.put("topic", exchange.getMessage().getHeader("CamelKafkaTopic", String.class));
        row.put("partition", exchange.getMessage().getHeader("CamelKafkaPartition", Integer.class));
        row.put("offset", exchange.getMessage().getHeader("CamelKafkaOffset", Long.class));
        Long timestamp = exchange.getMessage().getHeader("CamelKafkaTimestamp", Long.class);
        row.put("timestamp", timestamp == null ? null : new Timestamp(timestamp));
        row.put("key", exchange.getProperty("rawKeyBytes", byte[].class));
        row.put("value", exchange.getProperty("rawValueBytes", byte[].class));
        String headers = exchange.getProperty("rawHeaders", String.class);
        row.put("headers", headers == null || headers.isEmpty() ? "{}" : headers);

        String reason = text(exchange.getProperty("dlqReason", String.class));
        row.put("reason", reason == null ? "unexpected" : reason);
        row.put("field", text(exchange.getProperty("dlqField", String.class)));
        row.put("detail", text(exchange.getProperty("dlqDetail", String.class)));

        // The exception class only for an unexpected error: for a poison record it is always the same class and says nothing.
        Throwable caught = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class);
        row.put("exceptionClass", "unexpected".equals(row.get("reason")) && caught != null ? text(caught.getClass().getName()) : null);

        // An unexpected error was tried again app.unexpected.maxRetries times by the Dead Letter Channel before it got here. The redelivery counter
        // was not usable at this point (a record tried 4 times was recorded as attempt 1), so the attempt comes from the setting the policy reads.
        // A poison or refused record is not retried: attempt 1.
        row.put("attempt", "unexpected".equals(row.get("reason"))
                ? Integer.parseInt(exchange.getContext().resolvePropertyPlaceholders("{{app.unexpected.maxRetries}}")) + 1
                : 1);

        exchange.getMessage().setBody(row);
    }

    /** Without NULs and cut to MAX_TEXT characters; null stays null. */
    static String text(String value) {
        if (value == null) {
            return null;
        }
        String clean = value.replace("\u0000", "");
        return clean.length() <= MAX_TEXT ? clean : clean.substring(0, MAX_TEXT);
    }
}

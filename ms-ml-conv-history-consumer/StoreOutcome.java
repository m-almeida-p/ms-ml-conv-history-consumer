import java.util.List;
import java.util.Map;

import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;

/**
 * Reads the single row the store statement returns and decides what happened to the event:
 *
 *   stored     a new event: written to the inbox, and to the messages and summaries when it is a chat event
 *   duplicate  the inbox already holds this event: nothing written (a redelivery)
 *
 * When the inbox holds a different event at the same stream position, the record is refused for good: a
 * PoisonRecordException with reason store-rejected. Without this check the new event would pass for a
 * redelivery and be lost without a trace.
 */
@BindToRegistry("storeOutcome")
public class StoreOutcome {

    public void evaluate(Exchange exchange) throws PoisonRecordException {
        Object body = exchange.getMessage().getBody();
        if (body instanceof List<?> rows && !rows.isEmpty()) {
            body = rows.get(0);
        }
        if (!(body instanceof Map<?, ?> row)) {
            throw new IllegalStateException("the store statement returned no row");
        }

        long inserted = ((Number) row.get("inserted")).longValue();
        long projected = ((Number) row.get("projected")).longValue();
        boolean same = Boolean.TRUE.equals(row.get("same"));

        String outcome;
        if (inserted == 1) {
            outcome = "stored";
        } else if (same) {
            outcome = "duplicate";
        } else {
            exchange.setProperty("dlqReason", "store-rejected");
            exchange.setProperty("dlqField", "stream-position");
            exchange.setProperty("dlqDetail", "the stream position already holds another event");
            throw new PoisonRecordException(exchange, "store-rejected", "stream-position", "the stream position already holds another event");
        }

        boolean invalid = Boolean.TRUE.equals(exchange.getProperty("invalid", Boolean.class));
        exchange.getMessage().setHeader("outcome", outcome);
        exchange.getMessage().setHeader("projected", projected == 1);
        exchange.getMessage().setHeader("invalid", invalid && inserted == 1);
    }
}

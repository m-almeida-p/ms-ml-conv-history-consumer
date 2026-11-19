import org.apache.camel.Exchange;
import org.apache.camel.ValidationException;

/**
 * A record that can never be stored, however often it is tried again: it cannot be decoded, it breaks the
 * Schema Registry schema or the consumer's contract, or the store refuses it for good. The route sends it to
 * the dead-letter log and moves on, so one bad record never blocks a partition.
 *
 * It extends Camel's ValidationException on purpose: the route's onException matches that built-in class by
 * name, because a class compiled from this project's Java files cannot be resolved by name there.
 *
 * The reason is one of: header, no-schema-header, schema-id, schema-violation, json, field-missing,
 * field-malformed, field-range, payload, metadata, occurred-at, store-rejected. `unexpected` is the reason of a
 * record the rules did not foresee.
 */
public class PoisonRecordException extends ValidationException {

    private final String reason;
    private final String field;

    public PoisonRecordException(Exchange exchange, String reason, String field, String detail) {
        super(exchange, reason + (field == null || field.isEmpty() ? "" : " (" + field + ")") + ": " + detail);
        this.reason = reason;
        this.field = field == null ? "" : field;
    }

    public String getReason() {
        return reason;
    }

    public String getField() {
        return field;
    }
}

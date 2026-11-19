import java.sql.SQLException;
import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;

/**
 * Names the reason a record is dead-lettered when the store refuses its data for good (SQLSTATE class 22 or 23: a
 * constraint, or data a column cannot hold, such as a NUL in a text). The rule that calls it matches Spring's
 * DataIntegrityViolationException; the SQLSTATE is on the SQLException underneath, so this walks the causes.
 *
 * It is a bean and not a simple-language expression on purpose: "${exception.cause.SQLState}" cannot find the getter
 * (getSQLState) and fails, which made the handler itself fail and left the record unhandled and its offset uncommitted.
 *
 * Only the SQLSTATE is kept: Postgres puts the failing row in the error detail, and a chat text is personal data.
 */
@BindToRegistry("storeRefusal")
public class StoreRefusal {

    public void name(Exchange exchange) {
        String state = "unknown";
        for (Throwable t = exchange.getProperty(Exchange.EXCEPTION_CAUGHT, Throwable.class); t != null; t = t.getCause()) {
            if (t instanceof SQLException sql && sql.getSQLState() != null) {
                state = sql.getSQLState();
                break;
            }
        }
        exchange.setProperty("dlqReason", "store-rejected");
        exchange.setProperty("dlqField", "sqlstate-" + state);
        exchange.setProperty("dlqDetail", "the store refuses the data for good");
    }
}

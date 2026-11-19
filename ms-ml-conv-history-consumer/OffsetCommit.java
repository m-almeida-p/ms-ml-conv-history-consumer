import org.apache.camel.BindToRegistry;
import org.apache.camel.Exchange;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.consumer.KafkaManualCommit;

/**
 * Commits the offset of the record being handled. The route calls it only after the record was stored, or
 * dead-lettered, so a record that was not handled is never covered by a later commit (at-least-once
 * delivery, like ms-ml-conv-history-bs). The Kafka endpoint runs with autoCommitEnable=false and
 * allowManualCommit=true for this.
 */
@BindToRegistry("offsetCommit")
public class OffsetCommit {

    public void commit(Exchange exchange) {
        KafkaManualCommit manual = exchange.getMessage().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
        if (manual == null) {
            throw new IllegalStateException("no manual commit in the message: the Kafka endpoint needs allowManualCommit=true");
        }
        manual.commit();
    }
}

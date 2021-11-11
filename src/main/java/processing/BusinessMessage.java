package processing;

import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class BusinessMessage {

    private final String producerInfo;
    private final String messageId;
    private final String messageValue;

    public BusinessMessage(String producerInfo, String messageId) {
        this.producerInfo = producerInfo;
        this.messageId = messageId;
        messageValue = generateValueString();
    }

    private String generateValueString() {
        return Hashing.sha256().hashString(UUID.randomUUID().toString(), StandardCharsets.UTF_8).toString();
    }

    @Override
    public String toString() {
        return String.format("Message info: producer - %s, message id - %s,  message value - %s"
                , producerInfo, messageId, messageValue);
    }

}

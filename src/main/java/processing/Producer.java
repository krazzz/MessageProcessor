package processing;

import exceptions.IllegalPriorityException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Getter
@Slf4j
public class Producer implements Comparable<Producer>{

    private final int priority;
    private final Queue<BusinessMessage> messageQueue;

    public Producer(int priority, ConcurrentLinkedQueue<BusinessMessage> messageQueue) {
        log.info("Configuring new producer with priority {}", priority);
        if (priority <= 0 || priority > 4) {
            throw new IllegalPriorityException();
        }
        this.priority = priority;
        this.messageQueue = messageQueue;
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (!(o instanceof Producer)) {
            return false;
        }
        return ((Producer) o).getPriority() == priority;
    }

    @Override
    public int hashCode() {
        return priority;
    }

    @Override
    public int compareTo(Producer o) {
        return Integer.compare(priority, o.getPriority());
    }
}

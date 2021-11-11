import exceptions.IllegalPriorityException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import processing.Producer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ProducerTest {

    @Test()
    void producerCreationNegativeCaseTest() {
        //Case with 0 priority
        Assertions.assertThrows(IllegalPriorityException.class,
                () -> new Producer(0, new ConcurrentLinkedQueue<>()));
        //Case with negative priority
        Assertions.assertThrows(IllegalPriorityException.class,
                () -> new Producer(-123, new ConcurrentLinkedQueue<>()));
        //Case with priority greater allowed
        Assertions.assertThrows(IllegalPriorityException.class,
                () -> new Producer(5, new ConcurrentLinkedQueue<>()));
        //Case with priority much greater allowed
        Assertions.assertThrows(IllegalPriorityException.class,
                () -> new Producer(54545, new ConcurrentLinkedQueue<>()));
    }

    @Test
    void producerCreationPositiveCaseTest() {
        List<Producer> producers = new ArrayList<>(4);
        producers.add(new Producer(1, new ConcurrentLinkedQueue<>()));
        producers.add(new Producer(2, new ConcurrentLinkedQueue<>()));
        producers.add(new Producer(3, new ConcurrentLinkedQueue<>()));
        producers.add(new Producer(4, new ConcurrentLinkedQueue<>()));

        Assertions.assertEquals(producers.size(), 4);
    }
}

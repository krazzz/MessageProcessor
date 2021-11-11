import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import processing.BusinessMessage;
import processing.MessageProcessor;
import processing.Producer;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.*;

public class MessageProcessorTest {

    Producer producer1 = new Producer(1, new ConcurrentLinkedQueue<>());
    Producer producer2 = new Producer(2, new ConcurrentLinkedQueue<>());
    Producer producer3 = new Producer(3, new ConcurrentLinkedQueue<>());
    Producer producer4 = new Producer(4, new ConcurrentLinkedQueue<>());

    BlockingQueue<BusinessMessage> consumerQueue = new ArrayBlockingQueue<>(10);

    @BeforeEach
    void resetProducersState() {
        //Producer 1
        producer1.getMessageQueue().clear();
        producer1.getMessageQueue().add(new BusinessMessage("Producer 1", "Message 1"));
        producer1.getMessageQueue().add(new BusinessMessage("Producer 1", "Message 2"));
        producer1.getMessageQueue().add(new BusinessMessage("Producer 1", "Message 3"));
        producer1.getMessageQueue().add(new BusinessMessage("Producer 1", "Message 4"));

        //Producer 2
        producer2.getMessageQueue().clear();
        producer2.getMessageQueue().add(new BusinessMessage("Producer 2", "Message 1"));
        producer2.getMessageQueue().add(new BusinessMessage("Producer 2", "Message 2"));
        producer2.getMessageQueue().add(new BusinessMessage("Producer 2", "Message 3"));
        producer2.getMessageQueue().add(new BusinessMessage("Producer 2", "Message 4"));

        //Producer 3
        producer3.getMessageQueue().clear();
        producer3.getMessageQueue().add(new BusinessMessage("Producer 3", "Message 1"));
        producer3.getMessageQueue().add(new BusinessMessage("Producer 3", "Message 2"));
        producer3.getMessageQueue().add(new BusinessMessage("Producer 3", "Message 3"));
        producer3.getMessageQueue().add(new BusinessMessage("Producer 3", "Message 4"));

        //Producer 4
        producer4.getMessageQueue().clear();
        producer4.getMessageQueue().add(new BusinessMessage("Producer 4", "Message 1"));
        producer4.getMessageQueue().add(new BusinessMessage("Producer 4", "Message 2"));
        producer4.getMessageQueue().add(new BusinessMessage("Producer 4", "Message 3"));
        producer4.getMessageQueue().add(new BusinessMessage("Producer 4", "Message 4"));

        //Consumer queue
        consumerQueue.clear();
    }

    @Test
    void createOneProducerAndProcessMessagesTest() throws InterruptedException {
        //Given
        MessageProcessor messageProcessor = new MessageProcessor(Collections.singletonList(producer1), consumerQueue);

        //When
        messageProcessor.startMessageProcessing();

        //Then
        TimeUnit.SECONDS.sleep(2);
        messageProcessor.stopMessageProcessing();
        Assertions.assertEquals(0, producer1.getMessageQueue().size());
        Assertions.assertEquals(4, consumerQueue.size());
    }

    @Test
    void createTwoProducersWithDifferentPrioritiesTest() throws InterruptedException {
        //Given
        MessageProcessor messageProcessor = new MessageProcessor(Arrays.asList(producer1, producer4), consumerQueue);

        //When
        messageProcessor.startMessageProcessing();

        //Then
        TimeUnit.MILLISECONDS.sleep(200);
        messageProcessor.stopMessageProcessing();
        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(3, producer1.getMessageQueue().size());
        TimeUnit.MILLISECONDS.sleep(100);
        Assertions.assertEquals(5, consumerQueue.size());

    }

    @Test
    void createTwoProducersWithTheSamePriorityTest() {
        //Given - when
        MessageProcessor messageProcessor = new MessageProcessor(
                Arrays.asList(producer1, new Producer(1, new ConcurrentLinkedQueue<>()),
                        new Producer(1, new ConcurrentLinkedQueue<>())), consumerQueue);

        //Then
        Assertions.assertEquals(1, messageProcessor.getNumberOfActiveProducers());
    }

    @Test
    void createThreeProducersWithDifferentPrioritiesTest() throws InterruptedException {
        //Given
        MessageProcessor messageProcessor = new MessageProcessor(Arrays.asList(producer1, producer2, producer4), consumerQueue);

        //When
        messageProcessor.startMessageProcessing();

        //Then
        TimeUnit.MILLISECONDS.sleep(200);
        messageProcessor.stopMessageProcessing();
        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(2, producer2.getMessageQueue().size());
        Assertions.assertEquals(3, producer1.getMessageQueue().size());
        TimeUnit.MILLISECONDS.sleep(100);
        Assertions.assertEquals(7, consumerQueue.size());
    }

    @Test
    void createFourProducersWithDifferentPriorityTest() throws InterruptedException {
        //Given
        MessageProcessor messageProcessor = new MessageProcessor(Arrays.asList(producer1, producer2, producer3, producer4), consumerQueue);

        //When
        messageProcessor.startMessageProcessing();

        //Then
        TimeUnit.MILLISECONDS.sleep(200);
        messageProcessor.stopMessageProcessing();
        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(1, producer3.getMessageQueue().size());
        Assertions.assertEquals(2, producer2.getMessageQueue().size());
        Assertions.assertEquals(3, producer1.getMessageQueue().size());
        TimeUnit.MILLISECONDS.sleep(100);
        Assertions.assertEquals(10, consumerQueue.size());
    }

    @Test
    void createFourProducersReachingLimitOfConsumerQueueTest() throws InterruptedException {
        //Given
        MessageProcessor messageProcessor = new MessageProcessor(Arrays.asList(producer1, producer2, producer3, producer4), consumerQueue);

        //When
        messageProcessor.startMessageProcessing();

        //Then
        TimeUnit.MILLISECONDS.sleep(400);

        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(0, producer3.getMessageQueue().size());
        Assertions.assertEquals(0, producer2.getMessageQueue().size());
        Assertions.assertEquals(2, producer1.getMessageQueue().size());
        Assertions.assertEquals(10, consumerQueue.size());

        consumerQueue.clear();

        TimeUnit.MILLISECONDS.sleep(700);
        messageProcessor.stopMessageProcessing();
        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(0, producer3.getMessageQueue().size());
        Assertions.assertEquals(0, producer2.getMessageQueue().size());
        Assertions.assertEquals(0, producer1.getMessageQueue().size());
        Assertions.assertEquals(6, consumerQueue.size());
    }

    @Test
    void createTwoProducersWithDifferentPrioritiesHelpingLowerTest() throws InterruptedException {
        //Given
        MessageProcessor messageProcessor = new MessageProcessor(Arrays.asList(producer1, producer4), consumerQueue);

        //When
        messageProcessor.startMessageProcessing();
        messageProcessor.setNeighborsProcessingEnabled();

        //Then
        TimeUnit.MILLISECONDS.sleep(600);
        messageProcessor.stopMessageProcessing();
        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(0, producer1.getMessageQueue().size());
        Assertions.assertEquals(8, consumerQueue.size());
    }

    @Test
    void createTroProducersWithDifferentPriorityHelpingHigherTest() throws InterruptedException {
        //Given
        MessageProcessor messageProcessor = new MessageProcessor(Arrays.asList(producer1, producer4), consumerQueue);
        producer1.getMessageQueue().clear();
        producer4.getMessageQueue().add(new BusinessMessage("Producer 4", "Message 5"));

        //When
        messageProcessor.startMessageProcessing();
        messageProcessor.setNeighborsProcessingEnabled();

        //Then
        TimeUnit.MILLISECONDS.sleep(100);
        messageProcessor.stopMessageProcessing();

        TimeUnit.MILLISECONDS.sleep(200);
        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(0, producer1.getMessageQueue().size());
        Assertions.assertEquals(5, consumerQueue.size());
    }

    @Test
    void messageProcessorLoadTest() throws InterruptedException {
        //Given
        BlockingQueue<BusinessMessage> consQueue = new ArrayBlockingQueue<>(1000);

        for (int i = 5; i <= 1000; ++i) {
            producer1.getMessageQueue().add(new BusinessMessage("Producer 1", "Message " + i));
            producer2.getMessageQueue().add(new BusinessMessage("Producer 2", "Message " + i));
            producer3.getMessageQueue().add(new BusinessMessage("Producer 3", "Message " + i));
            producer4.getMessageQueue().add(new BusinessMessage("Producer 4", "Message " + i));
        }

        MessageProcessor messageProcessor = new MessageProcessor(
                Arrays.asList(producer1, producer2, producer3, producer4),
                consQueue, 1);

        //When
        messageProcessor.startMessageProcessing();

        //Then
        TimeUnit.MILLISECONDS.sleep(300);
        Assertions.assertEquals(1000, consQueue.size());

        consQueue.clear();
        TimeUnit.MILLISECONDS.sleep(300);
        Assertions.assertEquals(1000, consQueue.size());

        consQueue.clear();
        TimeUnit.MILLISECONDS.sleep(300);
        Assertions.assertEquals(1000, consQueue.size());

        consQueue.clear();
        TimeUnit.MILLISECONDS.sleep(2000); //last step process remaining messages much longer on my machine. Probably due to GC STW
        Assertions.assertEquals(1000, consQueue.size());


        Assertions.assertEquals(0, producer4.getMessageQueue().size());
        Assertions.assertEquals(0, producer3.getMessageQueue().size());
        Assertions.assertEquals(0, producer2.getMessageQueue().size());
        Assertions.assertEquals(0, producer1.getMessageQueue().size());

    }
}

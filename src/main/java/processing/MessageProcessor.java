package processing;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class MessageProcessor {

    private final AtomicBoolean processingInProgress = new AtomicBoolean(false);
    private final AtomicBoolean neighborsProcessingEnabled = new AtomicBoolean(false);

    private final ExecutorService threadPool = Executors.newFixedThreadPool(10);
    private final int businessLogicMillisToWait;

    private final NavigableSet<Producer> producers = new TreeSet<>();

    private final BlockingQueue<BusinessMessage> consumerQueue;

    public MessageProcessor(List<Producer> producers, BlockingQueue<BusinessMessage> consumerQueue) {
        this.producers.addAll(producers);
        this.consumerQueue = consumerQueue;
        businessLogicMillisToWait = 250;
    }

    public MessageProcessor(List<Producer> producers, BlockingQueue<BusinessMessage> consumerQueue, int businessLogicMillisToWait) {
        this.producers.addAll(producers);
        this.consumerQueue = consumerQueue;
        this.businessLogicMillisToWait = businessLogicMillisToWait;
    }

    private void initiateMessageProcessing() {
        producers.forEach(producer -> {
            log.info("Starting message processing for producer with priority {}", producer.getPriority());
            for (int i = 1; i <= producer.getPriority(); ++i) {
                threadPool.submit(() -> {
                    log.debug("Starting thread for producer {}", producer.getPriority());
                    while (processingInProgress.get()) {
                        BusinessMessage msg = producer.getMessageQueue().poll();
                        if (msg != null) {
                            try {
                                enqueueConsumerMessage(executeBusinessLogic(msg));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        } else {
                            try {
                                if (neighborsProcessingEnabled.get()) {
                                    helpNeighbors(producer);
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
        });
    }

    /**
     * Helping higher and lower neighbors.
     * Algorithm works in a next way:
     * - We are getting neighbor with higher priority and checking messages. Help if neighbor and messages exist.
     * If neighbor doesn't exist. Check neighbor with lower priority.
     * - After processing messages = number of our threads quit to check own queue.
     */
    private void helpNeighbors(Producer currentProducer) throws InterruptedException {

        Producer producer = currentProducer;
        BusinessMessage msg = null;

        //Checking higher neighbors
        do {
            Producer higherNeighbor = producers.higher(producer);
            if (higherNeighbor != null) {
                msg = higherNeighbor.getMessageQueue().poll();
                producer = higherNeighbor;
            } else {
                break;
            }
        } while (msg != null);

        //Process messages if found
        if (msg != null) {
            log.debug("Helping higher producer with message. My priority {}, message: {}", currentProducer.getPriority(), msg);
            enqueueConsumerMessage(executeBusinessLogic(msg));
            return;
        }

        //Checking higher neighbors
        producer = currentProducer;
        do {
            Producer lowerProducer = producers.lower(producer);
            if (lowerProducer != null) {
                msg = lowerProducer.getMessageQueue().poll();
                producer = lowerProducer;
            } else {
                break;
            }
        } while (msg != null);

        //Process messages if found
        if (msg != null) {
            log.debug("Helping lower producer with message. My priority {}, message: {}", currentProducer.getPriority(), msg);
            enqueueConsumerMessage(executeBusinessLogic(msg));
        }
    }

    /**
     * Primary business logic

     */
    private BusinessMessage executeBusinessLogic(BusinessMessage message) throws InterruptedException {
        log.debug("Executing business logic for message {}.", message.toString());
        TimeUnit.MILLISECONDS.sleep(businessLogicMillisToWait);
        return message;
    }

    private void enqueueConsumerMessage(BusinessMessage msg) {
        try {
            consumerQueue.put(msg);
            log.debug("Message successfully processed {}", msg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void setNeighborsProcessingEnabled() {
        neighborsProcessingEnabled.set(true);
    }

    public void setNeighborsProcessingDisabled() {
        neighborsProcessingEnabled.set(false);
    }

    public int getNumberOfActiveProducers() {
        return producers.size();
    }

    public void startMessageProcessing() {
        log.info("Staring message processing.");
        processingInProgress.set(true);
        initiateMessageProcessing();
    }

    public void stopMessageProcessing() {
        log.info("Stopping message processing.");
        processingInProgress.set(false);
    }

}

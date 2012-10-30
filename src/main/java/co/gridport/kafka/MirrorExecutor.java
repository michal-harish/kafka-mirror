package co.gridport.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Blacklist;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.javaapi.producer.ProducerData;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Partitioned Kafka Mirror 
 * 
 * This class represents an executor for mirroring between source and destination
 * kafka clusters with a single partitioning strategy provided via MirrorEncoder
 * implementation. Within the executor instance, there are n consumer streams 
 * all using a single, partitioned, producer and all the source topics are 
 * 
 * A mirror application can instantiate multiple Mirror Exectors
 * if it mirrors multiple source clusters onto a single destination cluster.
 * 
 * @author Michal Harish
 *
 */

public class MirrorExecutor {

    static private Logger log = LoggerFactory.getLogger(MirrorExecutor.class);

    private Properties consumerProps;

    private MirrorResolver resolver;    
    private TopicFilter sourceTopicFilter;  
    private ConsumerConnector consumer;

    private Properties producerProps;
    private Producer<Integer, Message> producer;
    private Class<? extends MirrorResolver> resolverClass;

    private ExecutorService executor;

    protected long srcCount;
    protected long srcCountSnapshot;
    protected long destCount;
    protected long destCountSnapshot;
    protected long snapshotTimestamp;

    protected static int maxPartitions = 0;

    private boolean started = false; 

    /**
     * Constructor
     * 
     * @param sourceTopicFilter Source topic Whitelist or Blacklist filter
     * @param consumerProperties Kafka consumer properties 
     * @param producerProperties Kafka Producer properties
     * @param resolverClassName
     * @throws Exception 
     * @throws ClassNotFoundException
     */
    public MirrorExecutor(
        Properties consumerProperties,
        Properties producerProperties,
        Class<? extends MirrorResolver> resolverClass
    ) throws Exception 
    {
        if (consumerProperties.containsKey("topics.whitelist")) {
            sourceTopicFilter = new Whitelist(consumerProperties.getProperty("topics.whitelist"));
        } else if (consumerProperties.containsKey("topics.blacklist")) {
            sourceTopicFilter = new Blacklist(consumerProperties.getProperty("topics.blacklist"));
        } else {
            throw new Exception("Consumer must have either topics.whitelist " +
        		"or topics.blacklist property set to a coma-separated list of topics"
            );
        }
        this.resolverClass = resolverClass;
        this.consumerProps = consumerProperties;
        this.producerProps = producerProperties;
    }

    /**
     * Start the mirror streaming of the messages and return
     * true for success and false when failed.  
     */
    public boolean start()
    {
        if (started)
        {
            return true;
        }
        log.info("Initializing Kafka Mirror Executor");
        log.info("Mirror soruce ZK: " + consumerProps.get("zk.connect"));
        log.info("Mirror source topics: " + this.sourceTopicFilter.toString());
        log.info("Mirror source backoff sleep: " + consumerProps.get("backoff.increment.ms"));
        log.info("Mirror dest ZK: " + producerProps.get("zk.connect"));
        log.info("Mirror dest queue time:" + producerProps.get("queue.time"));
        log.info("Mirror resolver class:" + resolverClass.getName());

        //instantiate resolver
        try {
            resolver = resolverClass.newInstance();
        } catch (Exception e1) {
            log.error("Could not instantiate resolver " + resolverClass.getName(), e1);
            return false;
        } 

        //prepare producer
        try {
            producerProps.put("partitioner.class", MirrorPartitioner.class.getName());
            producer = new Producer<Integer, Message>(new ProducerConfig(producerProps));     
        } catch (Exception e)
        {
            log.warn("Mirror producer failed: " + e.getMessage());
            return false;
        }

        //create consumer streams 
        try {
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
        } catch (Exception e)
        {
            log.warn("Mirror consumer connector failed: " + e.getMessage());
            return false;
        }

        List<KafkaStream<Message>> streams = consumer.createMessageStreamsByFilter(sourceTopicFilter);

        log.info("Mirror consumer executor pool = " + streams.size());

        //register shutdown hook
        started = true;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.debug("MIRROR SHUTDOWN SIGNALLED");
                cleanup();
            }
        });

        executor = Executors.newFixedThreadPool(streams.size());

        snapshotTimestamp = System.currentTimeMillis();

        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new Runnable() {
                public void run() {
                    log.debug("KAFKA MIRROR EXECUTOR TASK LISTENING FOR MESSAGES");
                    ConsumerIterator<Message> it = stream.iterator();
                    while(it.hasNext())
                    {
                        MessageAndMetadata<Message> metaMsg = it.next();

                        log.debug("GOT MESSAGE IN TOPIC " + metaMsg.topic());
                        List<MirrorDestination> destList = resolver.resolve(metaMsg);
                        srcCount++;

                        ArrayList<Message> messageList = new ArrayList<Message>();
                        messageList.add(metaMsg.message());

                        List<ProducerData<Integer, Message>> dataForMultipleTopics 
                            = new ArrayList<ProducerData<Integer, Message>>();
                        for(MirrorDestination dest: destList)
                        {
                            destCount++;
                            ProducerData<Integer,Message> dataForSingleTopic 
                                = new ProducerData<Integer,Message>(
                                    dest.getTopic(),
                                    dest.getKey(),
                                    messageList
                                )
                            ;
                            dataForMultipleTopics.add(dataForSingleTopic);
                            if (dest.getKey() == null)
                            {
                                log.debug("ADDING MESSAGE TO TOPIC " + dest.getTopic() + " WITH RANDOM PARTITIONING");
                            }
                            else
                            {
                                log.debug("ADDING MESSAGE TO TOPIC " + dest.getTopic() + " WITH PARTITIONING KEY " + dest.getKey());
                            }
                        }

                        producer.send(dataForMultipleTopics);
                    } 
                }
            });
        }
        return true;
    }

    /**
     * @return TRUE If the mirror executor is running
     */
    public boolean started()
    {
        return started;
    }

    /**
     * Get mirror throughput metrics.
     * 
     * @return Stream statistics in a single line format
     */
    public String getStats()
    {
        long secondsElapsed = (System.currentTimeMillis() - snapshotTimestamp) / 1000;
        snapshotTimestamp = System.currentTimeMillis();

        long srcCountPerSecond = (srcCount - srcCountSnapshot) / secondsElapsed;
        srcCountSnapshot = srcCount;

        long destCountPerSecond = (destCount - destCountSnapshot) / secondsElapsed;
        destCountSnapshot = destCount;

        long numPartitions = maxPartitions;
        maxPartitions = 0;

        return "src/sec=" + srcCountPerSecond+
            ", dest/sec=" + destCountPerSecond+
            ", high.partition=" + numPartitions +
            " " + consumerProps.get("zk.connect");
    }

    /**
     * Graceful shutdown of the mirror execution.
     */
    public void shutdown()
    {
        if (started)
        {
            executor.shutdown();
            try {
                while(!executor.isTerminated())
                {
                    executor.awaitTermination(1, TimeUnit.MINUTES);
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
            }
        }
        cleanup();
    }

    /**
     * Internal method for closing connections.
     */
    private void cleanup()
    {
        if (producer != null)
        {
            log.debug("CLOSING MIRROR PRODUCER");
            producer.close();
            producer = null;
        }
        if (consumer != null)
        {
            log.debug("CLOSING MIRROR CONSUMER CONNECTOR");
            consumer.shutdown();
            consumer = null;
        }
        started = false;
    }
}

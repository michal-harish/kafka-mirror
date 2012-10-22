package co.gridport.kafka;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
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
 * @author michal.harish
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
    
    private ExecutorService executor;
    
    protected long srcCount;
    protected long srcCountSnapshot;
    protected long destCount;
    protected long destCountSnapshot;
    protected long snapshotTimestamp;
    
    private boolean started = false; 

    /**
     * 
     * @param sourceZk Source Cluster ZooKeeper Connection String
     * @param consumerGroup
     * @param sourceZkTimeout Source Cluster ZooKeeper connection timeout ms
     * @param sourceTopicFilter Source topic Whitelist or Blacklist filter
     * @param destZk Destination Clustoer ZooKeeper Connection String
     * @param destResolverClass
     * @param maxLatency Maximum total mirroring footprint in ms 
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public MirrorExecutor(        
        String sourceZk,
        long sourceZkTimeout,
        TopicFilter sourceTopicFilter,
        String consumerGroup,
        Class<? extends MirrorResolver> destResolverClass,
        String destZk,        
        long maxLatency
    ) throws InstantiationException, IllegalAccessException 
    {
        String halfMaxLatency = String.valueOf(Math.round(maxLatency / 2));
        
        consumerProps = new Properties();  
        consumerProps.put("zk.connect", sourceZk);
        consumerProps.put("zk.connectiontimeout.ms", String.valueOf(sourceZkTimeout));
        consumerProps.put("groupid", consumerGroup);
        consumerProps.put("backoff.increment.ms", halfMaxLatency);
        
        this.sourceTopicFilter = sourceTopicFilter;
        
        producerProps = new Properties();         
        producerProps.put("zk.connect", destZk);
        producerProps.put("producer.type", "async");
        producerProps.put("queue.time", halfMaxLatency );
        producerProps.put("compression.codec", "1");
        producerProps.put("partitioner.class", MirrorPartitioner.class.getName());
        
        resolver = destResolverClass.newInstance();

    }
    
    public void start()
    {         
        if (started)
        {
            log.info("Kafka Mirror Executor already running.");
            return;
        }
        log.info("Initializing Kafka Mirror Executor");
        log.info("Mirror soruce ZK: " + consumerProps.get("zk.connect"));               
        log.info("Mirror source topics: " + this.sourceTopicFilter.toString());
        log.info("Mirror source backoff sleep: " + consumerProps.get("backoff.increment.ms"));
        log.info("Mirror dest ZK: " + producerProps.get("zk.connect"));                               
        log.info("Mirror dest queue time:" + producerProps.get("queue.time"));
                
        //prepare producer
        try {
            producer = new Producer<Integer, Message>(new ProducerConfig(producerProps));     
        } catch (Exception e)
        {
            log.warn("Mirror producer failed: " + e.getMessage());
            return;
        }

        //create consumer streams 
        try {
            consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
        } catch (Exception e)
        {
            log.warn("Mirror consumer connector failed: " + e.getMessage());
            return;
        }
                
        List<KafkaStream<Message>> streams = consumer.createMessageStreamsByFilter(sourceTopicFilter);
        
        log.info("Mirror consumer executor pool = " + streams.size());
        
        //register shutdown hook
        started = true;
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.debug("Mirror shutdown signalled");
                shutdown();
            }            
        });        
        
        executor = Executors.newFixedThreadPool(streams.size());
        
        snapshotTimestamp = System.currentTimeMillis();
        
        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new Runnable() {
                public void run() {
                    log.info("Kafka Mirror input task listening for messages");
                    ConsumerIterator<Message> it = stream.iterator();                    
                    while(it.hasNext())
                    {
                        MessageAndMetadata<Message> metaMsg = it.next();                     
                        //decode the message and resolve the destination topic-partitionHash                        
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
                                    dest.getHash(),
                                    messageList
                                )
                            ;
                            dataForMultipleTopics.add(dataForSingleTopic);
                            log.debug("ADDING MESSAGE WITH HASH " + dest.getHash()+ " TO TOPIC " + dest.getTopic());
                        }                        
                        
                        producer.send(dataForMultipleTopics);
                    } 
                }
            });
        }
    }
    
    public boolean started()
    {
        return started;
    }
    
    public String getStats()
    {
        long secondsElapsed = (System.currentTimeMillis() - snapshotTimestamp) / 1000;
        snapshotTimestamp = System.currentTimeMillis();
        
        long srcCountPerSecond = (srcCount - srcCountSnapshot) / secondsElapsed;
        srcCountSnapshot = srcCount;
        
        long destCountPerSecond = (destCount - destCountSnapshot) / secondsElapsed;
        destCountSnapshot = destCount;        
        
        return  "src/sec=" + srcCountPerSecond+", dest/sec=" + destCountPerSecond;
        
    }
    
    public void join()
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
        shutdown();
    }
    
    private void shutdown()
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

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
 * @param <V> Partitioner Value type of the message
 */

public class MirrorExecutor <V> {
    
    static private Logger log = LoggerFactory.getLogger(MirrorExecutor.class);
        
    private Properties consumerProps;
    
    private MirrorResolver resolver;    
    private TopicFilter sourceTopicFilter;  
    private ConsumerConnector consumer;
        
    private Properties producerProps;
    private Producer<Integer, Message> producer;
    
    protected long srcCount;
    protected long destCount;

    /**
     * 
     * @param sourceZk
     * @param consumerGroup
     * @param sourceTopicFilter
     * @param destZk
     * @param destResolverClass
     * @param maxLatency Maximum total mirroring footprint in ms 
     * @throws InstantiationException
     * @throws IllegalAccessException
     */
    public MirrorExecutor(        
        String sourceZk,
        String consumerGroup,
        TopicFilter sourceTopicFilter,
        String destZk,        
        Class<? extends MirrorResolver> destResolverClass,
        long maxLatency
    ) throws InstantiationException, IllegalAccessException 
    {
        String halfMaxLatency = String.valueOf(Math.round(maxLatency / 2));
        
        consumerProps = new Properties();  
        consumerProps.put("zk.connect", sourceZk);
        consumerProps.put("zk.connectiontimeout.ms", "10000");
        consumerProps.put("groupid", consumerGroup);
        consumerProps.put("backoff.increment.ms", halfMaxLatency);
        
        this.sourceTopicFilter = sourceTopicFilter;
        
        producerProps = new Properties();         
        producerProps.put("zk.connect", destZk);
        producerProps.put("producer.type", "async");
        producerProps.put("batch.size", "1000");
        producerProps.put("queue.time", halfMaxLatency );
        producerProps.put("compression.codec", "1");
        producerProps.put("partitioner.class", MirrorPartitioner.class.getName());
        
        resolver = destResolverClass.newInstance();

    }

    protected void shutdown()
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
    }
    
    public void run()
    {           
        log.info("Initializing Kafka Mirror Executor");
        log.info("Mirror soruce ZK: " + consumerProps.get("zk.connect"));               
        log.info("Mirror source topics: " + this.sourceTopicFilter.toString());
        log.info("Mirror source backoff sleep: " + consumerProps.get("backoff.increment.ms"));
        log.info("Mirror dest ZK: " + producerProps.get("zk.connect"));                               
        log.info("Mirror dest queue time:" + producerProps.get("queue.time"));
        
        //register shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                log.debug("Mirror shutdown signalled");
                shutdown();
            }            
        });
        
        //prepare producer
        producer = new Producer<Integer, Message>(new ProducerConfig(producerProps));     

        //create consumer streams 
        consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(consumerProps));
        
        List<KafkaStream<Message>> streams = consumer.createMessageStreamsByFilter(sourceTopicFilter);
        
        log.info("Mirror consumer executor pool = " + streams.size());
        
        ExecutorService executor = Executors.newFixedThreadPool(streams.size());
        
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
                        MirrorDestination dest = resolver.resolve(metaMsg);
                        srcCount++;
                        
                        ArrayList<Message> messageList = new ArrayList<Message>();
                        messageList.add(metaMsg.message());
                        
                        List<ProducerData<Integer, Message>> dataForMultipleTopics 
                            = new ArrayList<ProducerData<Integer, Message>>();                        
                        for(String destTopic: dest.getTopics())
                        {
                            destCount++;
                            ProducerData<Integer,Message> dataForSingleTopic 
                                = new ProducerData<Integer,Message>(
                                    destTopic,
                                    dest.getHash(),
                                    messageList
                                )
                            ;
                            dataForMultipleTopics.add(dataForSingleTopic);
                            log.debug("ADDING MESSAGE WITH HASH " + dest.getHash()+ " TO TOPIC " + destTopic);
                        }                        
                        
                        producer.send(dataForMultipleTopics);
                    } 
                }
            });
        }
                
        try {
            executor.shutdown();
            while(!executor.isTerminated())
            {
                long srcCountSnapshot = srcCount;
                long destCountSnapshot = destCount;
                executor.awaitTermination(1, TimeUnit.MINUTES);
                long srcCountPerMinute = srcCount - srcCountSnapshot;
                long destCountPerMinute = destCount - destCountSnapshot;
                log.info("Kafka Mirror stats: " +
            		"src/sec=" + (srcCountPerMinute / 60) +
            		", dest/sec=" + (destCountPerMinute / 60)
        		);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }

        shutdown();
    }
}

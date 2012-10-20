package co.gridport.kafka;

import java.nio.ByteBuffer;
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
 * @author michal.harish@gmail.com
 *
 * @param <K> Partitioner Key type
 */

public class MirrorExecutor <V> {
    
    static private Logger log = LoggerFactory.getLogger(MirrorExecutor.class);
        
    private Properties consumerProps;
    
    private MirrorEncoder<V> encoder;    
    private TopicFilter sourceTopicFilter;       
        
    private Properties producerProps;

    public MirrorExecutor(        
        String sourceZk,
        String consumerGroup,
        TopicFilter sourceTopicFilter,
        String destZk,        
        Class<? extends MirrorEncoder<V>> destEncoderClass
    ) throws InstantiationException, IllegalAccessException 
    {
        consumerProps = new Properties();  
        consumerProps.put("zk.connect", sourceZk);
        consumerProps.put("zk.connectiontimeout.ms", "10000");
        consumerProps.put("groupid", consumerGroup);
        
        this.sourceTopicFilter = sourceTopicFilter;
        
        producerProps = new Properties();         
        producerProps.put("zk.connect", destZk);
        producerProps.put("producer.type", "async");
        producerProps.put("batch.size", "1000");
        producerProps.put("queue.time", "250");
        producerProps.put("compression.codec", "1");
        producerProps.put("partitioner.class", MirrorPartitioner.class.getName());
        producerProps.put("serializer.class", destEncoderClass.getName());
        
        encoder = destEncoderClass.newInstance();

    }
    
    public void run()
    {               
        log.debug("SOURCE ZK: " + consumerProps.get("zk.connect"));               
        log.debug("SOURCE TOPIC: " + this.sourceTopicFilter.toString());
        log.debug("DEST ZK: " + producerProps.get("zk.connect"));               
        log.debug("DEST SERIALIZER CLASS: " + producerProps.get("serializer.class"));
        log.debug("DEST PARTITONER CLASS: " + producerProps.get("partitioner.class"));
        
        //prepare producer
        final Producer<Integer, V> producer 
            = new Producer<Integer, V>(new ProducerConfig(producerProps));     

        //create consumer streams 
        ConsumerConfig consumerConfig = new ConsumerConfig(consumerProps);
        ConsumerConnector consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);    
        List<KafkaStream<Message>> streams = 
            consumerConnector.createMessageStreamsByFilter(sourceTopicFilter);
        
        log.debug("MIRROR CONSUMER POOL SIZE = " + streams.size());
        ExecutorService executor = Executors.newFixedThreadPool(streams.size());
        for(final KafkaStream<Message> stream: streams) {
            executor.submit(new Runnable() {
                public void run() {
                    log.debug("LISTENING FOR MESSAGES...");
                    ConsumerIterator<Message> it = stream.iterator();                    
                    while(it.hasNext())
                    {
                        MessageAndMetadata<Message> metaMsg = it.next();
                        //read message meta data
                        String topic = metaMsg.topic();                        
                        //read raw source message from the stream iterator
                        Message message = metaMsg.message();
                        ByteBuffer buffer = message.payload();
                        byte [] bytes = new byte[buffer.remaining()];
                        buffer.get(bytes);            
                        //decode, hash and send destination message
                        V destMessage = encoder.decode(bytes);
                        log.debug("GOT MESSAGE `" + (String) destMessage + "` IN TOPIC " + topic );
                        Integer hash= encoder.toHash(destMessage);
                        ArrayList<V> messageList = new ArrayList<V>();
                        messageList.add(destMessage);
                        ProducerData<Integer,V> data = new ProducerData<Integer,V>(
                            topic,
                            hash,
                            messageList
                        );
                        log.debug("SENDING DEST MESSAGE FOR HASH " + hash);
                        producer.send(data);
                    } 
                    log.debug("FINISHED");      
                }
            });
        }
        
        try {
            while(!executor.isTerminated())
            {
                executor. awaitTermination(1, TimeUnit.MINUTES);
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        
        producer.close();
        consumerConnector.shutdown();
    }
}

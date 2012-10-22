package co.gridport.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import kafka.consumer.Whitelist;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an example application of Kafka Paritioned Mirror,
 * where there are two remote source clusters each containing two topics 
 * that are both mirrored with the same resolver strategy onto the
 * single destination cluster. 
 * 
 * Also in this example the MirrorResolver pushes each message
 * into its original source topic as well as into an aggregate topic
 * called 'monitor' so each message appears on the destination cluster
 * twice.
 * 
 * The mirror should be deployed at the destination cluster.
 * 
 * @author michal.harish
 */

public class MirrorExample {
    
    static private Logger log = LoggerFactory.getLogger(MirrorExample.class);

    /**
     * @param args
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws InterruptedException 
     */
    public static void main(String[] args) throws InstantiationException, IllegalAccessException, InterruptedException
    {        
        String destZk = "localhost:2181";
        String mirrorConsumerGroup = "central_mirror";
        long maxLatency = 500;
        
        /* 
         * TODO load propertie from /etc/kafka/mirror.properties copying all mirror.n.consumer
         * properties into a separate exectutor.
         * 
        Properties props = new Properties();
        props.put("mirror.producer.zk.connect", destZk);
        props.put("mirror.producer.producer.type", "async");
        props.put("mirror.producer.queue.time", "250" );
        props.put("mirror.producer.compression.codec", "1");
        props.put("mirror.1.consumer.zk.connect", "aos3.gridport.co:2181");
        props.put("mirror.1.consumer.zk.connectiontimeout.ms", "10000");
        props.put("mirror.1.consumer.backoff.increment.ms", "250");
        props.put("mirror.1.consumer.groupid", mirrorConsumerGroup);
        props.put("mirror.1.topics.whitelist", "topic1,topic2");        
        props.put("mirror.2.consumer.zk.connect", "aos1.gridport.co:2181");
        props.put("mirror.2.consumer.zk.connectiontimeout.ms", "3000");
        props.put("mirror.2.consumer.backoff.increment.ms", "250");
        props.put("mirror.2.consumer.groupid", mirrorConsumerGroup);
        props.put("mirror.2.topics.whitelist", "topic1,topic2");        
        
        for(Object key: props.keySet())
        {
            String propertyKey = (String) key;
            System.out.println(propertyKey);
        }
        System.exit(0);
        */
        
        MirrorExecutor mirror1 = new MirrorExecutor(            
            "aos3.gridport.co:2181"
            ,10000
            ,new Whitelist("topic1,topic2")
            ,mirrorConsumerGroup
            ,ExampleEncoder.class
            ,destZk
            ,maxLatency
        );
        mirror1.start();
        
        MirrorExecutor mirror2 =  new MirrorExecutor(            
            "aos1.gridport.co:2181"
            ,6000
            ,new Whitelist("topic1,topic2")
            ,mirrorConsumerGroup
            ,ExampleEncoder.class
            ,destZk
            ,maxLatency
        );                
        mirror2.start();
        
        Thread main = Thread.currentThread();
        while(!main.isInterrupted())
        {
            synchronized(main)
            {
                main.wait(1000*10);
            }
            if (mirror1.started())
            {
                log.info("Mirror 1 stats: " + mirror1.getStats());
            }
            if (mirror2.started())
            {
                log.info("Mirror 2 stats: " + mirror1.getStats());
            }
        }
        
        mirror1.shutdown();
        mirror2.shutdown();
              
    }
    
    public static class ExampleEncoder implements MirrorResolver
    {
        public List<MirrorDestination> resolve(MessageAndMetadata<Message> metaMsg)
        {
            ArrayList<MirrorDestination> result = new ArrayList<MirrorDestination>();
            
            //decode the message payload
            ByteBuffer buffer = metaMsg.message().payload();
            byte [] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            
            //mirror the message onto the same topic with a string hash partitioning
            String payload = new String(bytes);            
            Integer hash = Math.abs(payload.hashCode());          
            result.add(new MirrorDestination(metaMsg.topic(), hash));                        
            
            //mirror all messages onto an extra monitor topic without any partitioning key 
            result.add(new MirrorDestination("monitor"));
            
            return result;
        }
        
    }   
    
}

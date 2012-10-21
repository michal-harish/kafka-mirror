package co.gridport.kafka;

import java.nio.ByteBuffer;

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
 * @todo load properties from /etc/kafka/mirror.properties 
 * @todo mirror.topics.whitelist = ..., mirror.consumer.zk.connect =.., mirror.producer.zk.connect = ...
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
        
        mirror1.join();
        mirror2.join();
              
    }
    
    public static class ExampleEncoder implements MirrorResolver
    {
        public MirrorDestination resolve(MessageAndMetadata<Message> metaMsg)
        {
            //decode the message payload
            ByteBuffer buffer = metaMsg.message().payload();
            byte [] bytes = new byte[buffer.remaining()];
            buffer.get(bytes);
            //generate hash
            String payload = new String(bytes);            
            Integer hash = Math.abs(payload.hashCode());
            MirrorDestination result = new MirrorDestination(hash);
            //mirror the message onto the same topic 
            result.addTopic(metaMsg.topic());
            //mirror all messages onto an extra monitor topic
            result.addTopic("monitor");            
            return result;
        }
        
    }   
    
}

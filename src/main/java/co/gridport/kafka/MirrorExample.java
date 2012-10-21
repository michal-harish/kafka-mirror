package co.gridport.kafka;

import java.nio.ByteBuffer;

import kafka.consumer.Whitelist;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

/**
 * This is an example application of Kafka Paritioned Mirror,
 * where the source cluster contains two topics that are 
 * both mirrored with string hash partitioner onto the
 * destination cluster. 
 * 
 * @author michal.harish
 */

public class MirrorExample {

    /**
     * @param args
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     */
    public static void main(String[] args) throws InstantiationException, IllegalAccessException
    {        
        String sourceZk = "aos3.gridport.co:2181";
        String consumerGroup = "mirror1";        
        String destZk = "localhost:2181";
        
        MirrorExecutor<String> mirror1 = new MirrorExecutor<String>(            
            sourceZk
            ,consumerGroup
            ,new Whitelist("topic1,topic2")
            ,destZk
            ,ExampleEncoder.class
            ,500
        );
        mirror1.run();
              
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
            //mirror the topic exactly
            result.addTopic(metaMsg.topic());
            result.addTopic("monitor");
            
            return result;
        }
        
    }   
    
}

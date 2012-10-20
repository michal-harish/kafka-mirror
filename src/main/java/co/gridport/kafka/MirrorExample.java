package co.gridport.kafka;

import kafka.consumer.Whitelist;
import kafka.message.Message;

/**
 * This is an example application of Kafka Paritioned Mirror,
 * where the source cluster contains two topics that are 
 * both mirrored with string hash partitioner onto the
 * destination cluster. 
 * 
 * @author michal.harish@gmail.com
 *
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
        );
        mirror1.run();
              
    }
    
    public static class ExampleEncoder implements MirrorEncoder<String>
    {
        public int toHash(String data)
        {
            return Math.abs(data.hashCode());
        }
        public Message toMessage(String data) {            
            return new Message(data.getBytes());
        }
        public String decode(byte[] payload) {            
            return new String(payload);
        }
        
    }   
    
}

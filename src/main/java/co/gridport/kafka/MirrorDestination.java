package co.gridport.kafka;


/**
 * MirrorDestination represents an output mapping 
 * for a message. This is used in a List<MirrorDestination> form
 * to provide potentially n output topic with their own
 * partitioning strategy for a single input message.
 * 
 * @author michal.harish
 *
 */
public class MirrorDestination {
    
    private String topic;    
    private Integer key;
    
    /**
     * Constructor with partitioning key
     * 
     * @param topic
     * @param key
     */
    public MirrorDestination(String topic, Integer key)
    {
        this.topic = topic;
        this.key = key;        
    }
    
    /**
     * Constructor with topic only. This will
     * result in random partitioning.
     * 
     * @param topic
     */
    public MirrorDestination(String topic)
    {
        this.topic = topic;
        this.key = null;
    }
       
    /**
     * @return Topic part of this destination
     */
    public String getTopic()
    {
        return topic;
    }
    
    /**
     * @return Partitioning key of this destination
     */
    public Integer getKey()
    {
        return key;
    }
    
}

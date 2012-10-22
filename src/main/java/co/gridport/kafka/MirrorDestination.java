package co.gridport.kafka;


/**
 * MirrorDestination represents an ouput mapping 
 * for a hash-to-topics. This means that a message
 * set identified by a single hash can be directed
 * to multiple redundant topics.
 * 
 * @author michal.harish
 *
 */
public class MirrorDestination {
    
    private String topic;    
    private Integer hash;
    
    public MirrorDestination(String topic, Integer hash)
    {
        this.topic = topic;
        this.hash = hash;        
    }
    
    public MirrorDestination(String topic)
    {
        this.topic = topic;
        this.hash = null;
    }
        
    public String getTopic()
    {
        return topic;
    }
    
    public Integer getHash()
    {
        return hash;
    }
    
}

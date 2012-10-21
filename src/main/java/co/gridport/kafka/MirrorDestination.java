package co.gridport.kafka;

import java.util.ArrayList;

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
    
    private ArrayList<String> topics;    
    private Integer hash;
    
    public MirrorDestination(
        Integer hash
    )
    {
        this.hash = hash;
        topics = new ArrayList<String>();
    }
    
    public void addTopic(String topic)
    {
        topics.add(topic);
    }
    
    public ArrayList<String> getTopics()
    {
        return topics;
    }
    
    public Integer getHash()
    {
        return hash;
    }
    
    
}

package co.gridport.kafka;

import java.util.List;

import kafka.message.Message;
import kafka.message.MessageAndMetadata;


/**
 * Mirror Resolver interface is used for deciding where an
 * incoming message should go in the destination cluster
 * by implementing the partitioning and duplication strategy 
 * via resolve(..) method.
 * 
 * @author michal.harish
 * 
 */
public interface MirrorResolver {

    /**
     * resolve method will be called for each incoming message
     * and is expected to return the list of destinations onto
     * which the message will be mapped in the destination cluster.
     *   
     * @param metaMsg
     * @return
     */
    public List<MirrorDestination> resolve(
        MessageAndMetadata<Message> metaMsg
    );
    
}
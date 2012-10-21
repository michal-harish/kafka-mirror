package co.gridport.kafka;

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

    public MirrorDestination resolve(
        MessageAndMetadata<Message> metaMsg
    );
    
}
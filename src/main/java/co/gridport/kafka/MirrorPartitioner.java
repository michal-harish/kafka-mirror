package co.gridport.kafka;

import kafka.producer.Partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mirror Partitioner is a final implementation of kafka producer partitioner
 * that always expects an Integer hash as a partitioning key.
 * 
 * It is a responsibility of the MirrorEncoder implementation to provide
 * code which reduces the message into an integer hash by which the 
 * destination topic is then partitioned.
 * 
 * @author michal.harish@gmail.com
 * 
 */
public final class MirrorPartitioner implements Partitioner<Integer> {

    static private Logger log = LoggerFactory.getLogger(MirrorPartitioner.class);
    
    public int partition(Integer hash, int numPartitions) {
        int result =  hash % numPartitions;
        log.debug("PARTITIONING " + hash + " FOR " + numPartitions + " PARTITONS >> " + result);
        return result;
    }
}

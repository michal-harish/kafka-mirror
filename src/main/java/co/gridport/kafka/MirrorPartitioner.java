package co.gridport.kafka;

import java.util.Random;

import kafka.producer.Partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mirror Partitioner is a final implementation of kafka producer partitioner
 * that always expects an Integer hash as a partitioning key.
 * 
 * It is a responsibility of the MirrorResolver implementation to provide
 * logic which reduces the message into an integer hash by which the 
 * destination topic is then partitioned.
 * 
 * @author michal.harish
 * 
 */
public final class MirrorPartitioner implements Partitioner<Integer> {

    static private Logger log = LoggerFactory.getLogger(MirrorPartitioner.class);
    
    static Random generator = new Random();
    
    public int partition(Integer key, int numPartitions) {
        int result;
        if (key == null)
        {
            result = generator.nextInt(numPartitions);
        }
        else
        {
            result =  key % numPartitions;            
        }
        log.debug("PARTITIONING " + key + " FOR " + numPartitions + " PARTITONS >> " + result);
        if (numPartitions > MirrorExecutor.maxPartitions)
        {
            MirrorExecutor.maxPartitions = numPartitions;
        }
        return result;
    }
}

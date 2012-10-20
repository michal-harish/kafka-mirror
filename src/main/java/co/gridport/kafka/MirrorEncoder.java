package co.gridport.kafka;

import kafka.serializer.Encoder;

/**
 * Mirror Encoder interface is used for both decoding
 * the incoming messages from the mirror source topic filter
 * and for implementation of partitioning strategy 
 * via toHash(..) method.
 * 
 * @author michal.harish@gmail.com
 * 
 * @param <V> Message value type
 */
public interface MirrorEncoder<V> extends Encoder<V>{

    public V decode(byte[] payload);
    
    public int toHash(V message);
    
}
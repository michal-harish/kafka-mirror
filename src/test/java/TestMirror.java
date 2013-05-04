import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import kafka.message.MessageAndMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.gridport.kafka.Mirror;
import co.gridport.kafka.MirrorDestination;
import co.gridport.kafka.MirrorResolver;


public class TestMirror {

    static private Logger log = LoggerFactory.getLogger(TestMirror.class);

    public static void main(String[] args) throws Exception
    {
        Properties properties = new Properties();
        try {
            properties.load(
                TestMirror.class.getResourceAsStream("mirror.properties")
            );
        } catch (IOException e) {
            log.error("Could not load mirror.properties", e);
            System.exit(1);
        }

        Mirror mirror = new Mirror(properties);
        mirror.run(2);
    }

    public static class ExampleEncoder implements MirrorResolver
    {
        public List<MirrorDestination> resolve(MessageAndMetadata<Integer,String> metaMsg)
        {
            ArrayList<MirrorDestination> result = new ArrayList<MirrorDestination>();

            //mirror the message onto the same topic with a string hash partitioning
            String payload = metaMsg.message();
            Integer hash = Math.abs(payload.hashCode());
            result.add(new MirrorDestination(metaMsg.topic(), hash));

            //mirror all messages onto an extra monitor topic without any partitioning key 
            result.add(new MirrorDestination("monitor"));

            return result;
        }
    }  

}

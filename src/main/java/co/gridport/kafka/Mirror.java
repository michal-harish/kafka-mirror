package co.gridport.kafka;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mirror {
    
    static private Logger log = LoggerFactory.getLogger(Mirror.class);
    
    private ArrayList<MirrorExecutor> executors = new ArrayList<MirrorExecutor>();

    public static void main(String[] args)
    {
        if (args.length != 1)
        {
            String message = "Usage java -cp :/kafka-mirror.jar co.gridport.kafka.Mirror [properties-files]\n";
            log.warn(message);
            System.out.println(message);
            System.exit(1);
        }
        String propertiesFilename = args[0];
        Properties props = new Properties();
        try {
            props.load(new FileInputStream(propertiesFilename));
        } catch (IOException e) {
            log.error("Could not load mirror.properties", e);
            System.exit(1);
        }

        try {
            new Mirror(props).run(60);
        } catch (Exception e) {
            log.error("Failed to initialize and start the mirror", e);
            System.exit(1);
        }
    }
    
    /**
     * Constructor - initializes MirrorExecutors but doesn't start them.
     * 
     * @param props
     * @throws Exception When initialization fails due to misconfiguration or else
     */
    public Mirror(Properties props) throws Exception
    {

        //load resolver class configuration
        Class<? extends MirrorResolver> resolverClass = null;        
        try {
            if (!props.containsKey("resolver.class"))
            {
                throw new Exception("Property resolver.class missing in the mirror configuration.");
            }
            String resolverClassName = props.getProperty("resolver.class");
            resolverClass = Class.forName(resolverClassName).asSubclass(MirrorResolver.class);
        } catch (Exception e) {
            throw new Exception("Invalid mirror resolver class provided", e);
        }
        
        //prepare producer properties
        Properties producerProperties = new Properties();
        //prepare set of consumer properties
        HashMap<Integer,Properties> propertiesSet = new HashMap<Integer,Properties>();
        //transform mirror properties 
        for(Enumeration<Object> e = props.keys(); e.hasMoreElements();)
        {
            Object propKey = e.nextElement();            
            String propName = ((String) propKey);
            String propValue = (String) props.get(propKey);
            log.debug("parsing property " + propName+ " = " + propValue);
            if (propName.matches("^producer\\..*"))
            {
                producerProperties.put(propName.substring(9), propValue);
            } else if (propName.matches("^consumer\\.[0-9]+\\..*"))
            {
                propName = propName.substring(9);
                Integer mirrorId = Integer.valueOf(propName.substring(0,propName.indexOf(".")));
                propName = propName.substring(propName.indexOf(".")+1);                
                if (!propertiesSet.containsKey(mirrorId))
                {
                    propertiesSet.put(mirrorId, new Properties());
                }
                propertiesSet.get(mirrorId).put(propName, propValue);
            }
        }
        if (producerProperties.isEmpty())
        {
            throw new Exception("No mirror.producer.* properties found");
        }
        if (propertiesSet.isEmpty())
        {
            throw new Exception("No mirror.<N>.consumer.* properties found");            
        }

        //Instantiate mirror executors        
        for(Integer mirrorId: propertiesSet.keySet())
        {          
            log.info("Initializing mirror." + mirrorId);
            try {
                executors.add(
                    new MirrorExecutor(
                        propertiesSet.get(mirrorId),
                        producerProperties,
                        resolverClass
                    )
                );
            } catch (Exception e) {
                log.warn("Failed to create MirrorExecutor for mirror." + mirrorId, e);
            }
        }
    }


    /**
     * 
     * @param statFrequency Frequency in seconds at which stats will be logged
     */
    public void run(long statFrequency)
    {
        //start all executors
        int startedExecutors = 0;
        for(MirrorExecutor executor: executors){
            if(!executor.start()){
                log.warn("Failed to start mirror executor");                
            } else {
                startedExecutors++;
            }
        }
        log.info("Running mirror executors: " + startedExecutors);
        
        //observe mirror stats while running
        Thread main = Thread.currentThread();
        try {
            //SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("HH:mm:ss");
            while(!main.isInterrupted())
            {
                synchronized(main)
                {
                    main.wait(1000*statFrequency);
                }
                for(MirrorExecutor executor: executors)
                {
                    if (executor.started())
                    {
                        log.info("Mirror stats: " + executor.getStats());
                    }
                }
            }
        } catch (InterruptedException e) {
            log.warn("Mirror execution interruped.. ");
        }

        //stop all executors
        for(MirrorExecutor executor: executors)
        {
            executor.shutdown();
        }
    }
    
}

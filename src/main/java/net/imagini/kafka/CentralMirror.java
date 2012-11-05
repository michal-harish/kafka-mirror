package net.imagini.kafka;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import co.gridport.kafka.Mirror;


public class CentralMirror {
    static private Logger log = LoggerFactory.getLogger(CentralMirror.class);

    static private String defaultPropertiesLocation = "/etc/vdna/kafka/central-mirror.properties";

    //static public double latestObservedTimestamp = 0;
    //static public double earliestObservedTimestamp = Double.MAX_VALUE;

    public static void main(String[] args) throws Exception
    {
        Properties properties = new Properties();
        InputStream in = null;
        if (args.length == 0)
        {
            if (new File(defaultPropertiesLocation).exists())
            {
                in = new FileInputStream(defaultPropertiesLocation);
                log.info("Using default " + defaultPropertiesLocation);
            } else {
                in = CentralMirror.class.getResourceAsStream("central-mirror-dev.properties");
                log.info("Using embedded development central-mirror-dev.properties");
            }
        } else {
            String propertiesFilename = args[0];
            in = new FileInputStream(propertiesFilename);
            log.info("Using " + propertiesFilename);
        }

        try {
            properties.load(in);
            in.close();
        } catch (FileNotFoundException e1) {
            log.error(e1.getMessage());
            return;
        } catch (IOException e1) {
            log.error("Couuld not read properties files");
        }

        Mirror mirror = new Mirror(properties);

        mirror.run(10);
    }

}
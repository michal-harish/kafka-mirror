package co.gridport.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;

import kafka.consumer.Blacklist;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaMirror {

    static private Logger log = LoggerFactory.getLogger(KafkaMirror.class);

    static private Options options = new Options();
    /**
     * 
     * @param args
     * @throws IllegalAccessException 
     * @throws InstantiationException 
     * @throws ParseException 
     * @throws IOException 
     */
    public static void main(String[] args) 
        throws InstantiationException, IllegalAccessException, ParseException 
    {

        options.addOption("s", "skip", true, "Skip missed messages");
        options.addOption("h", "help", false, "Print help for this command");

        CommandLineParser parser = new PosixParser();
        CommandLine cmd = parser.parse(options, args);
        if (cmd.hasOption("h") || cmd.getArgs().length == 0)
        {
           printHelpAndExit();
        }

        String propertiesFilename = cmd.getArgs()[0];

        Properties props = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(propertiesFilename);
            props.load(in);
            in.close();
        } catch (FileNotFoundException e1) {
            log.error(e1.getMessage());
            return;
        } catch (IOException e1) {
            log.error("Couuld not read " + propertiesFilename);
        }

        Integer maxLatencyMs = Integer.valueOf(props.getProperty("latency.ms", "500"));

        String destZkConnection = props.getProperty("producer.zk.connect");

        String groupId = props.getProperty("groupid");

        //load mirror configurations
        HashMap<Integer,Properties> propertiesSet = new HashMap<Integer,Properties>();

        for(Enumeration<Object> e = props.keys(); e.hasMoreElements();)
        {
            Object propKey = e.nextElement();
            String propString = ((String) propKey).substring(7);
            if (propString.matches("^[0-9]+\\..*"))
            {
                Integer mirrorId = Integer.valueOf(propString.substring(0,propString.indexOf(".")));
                String propName = propString.substring(propString.indexOf(".")+1);
                String propValue = (String) props.get(propKey);
                if (!propertiesSet.containsKey(mirrorId))
                {
                    propertiesSet.put(mirrorId, new Properties());
                }
                propertiesSet.get(mirrorId).put(propName, propValue);
            }
        }

        //Instantiate mirror executors
        ArrayList<MirrorExecutor> mirrors = new ArrayList<MirrorExecutor>();
        for(Integer mirrorId: propertiesSet.keySet())
        {
            String sourceZk = null;
            Integer sourceZkTimeOut = 0;
            TopicFilter sourceFilter = null;
            Properties mirrorProperties = propertiesSet.get(mirrorId);
            for(Enumeration<Object> e = mirrorProperties.keys(); e.hasMoreElements();)
            {
                String propName = (String) e.nextElement();
                String propValue = (String) mirrorProperties.get(propName);
                if (propName.equals("zk.connect")) sourceZk = propValue;
                if (propName.equals("zk.connectiontimeout.ms")) sourceZkTimeOut = Integer.valueOf(propValue);
                if (propName.equals("topics.whitelist")) sourceFilter = new Whitelist(propValue);
                if (propName.equals("topics.blacklist")) sourceFilter = new Blacklist(propValue);
            }
            /*
            mirrors.add(
                new MirrorExecutor(
                    sourceZk
                    ,sourceZkTimeOut
                    ,sourceFilter
                    ,groupId
                    ,DestinationResolver.class
                    ,destZkConnection
                    ,maxLatencyMs
                )
            );*/
        }

        //start all executors
        for(MirrorExecutor mirror: mirrors)
        {
            mirror.start();
        }

        //observe mirror stats while running
        Thread main = Thread.currentThread();
        try {
            SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("HH:mm:ss");
            while(!main.isInterrupted())
            {
                synchronized(main)
                {
                    main.wait(1000*10);
                }
                for(MirrorExecutor mirror: mirrors)
                {
                    if (mirror.started())
                    {
                        log.info("Mirror stats: " + mirror.getStats());
                    }
                }
            }
        } catch (InterruptedException e) {
            log.warn("Mirror execturion interruped.. ");
        }

        //start all executors
        for(MirrorExecutor mirror: mirrors)
        {
            mirror.shutdown();
        }

    }

    /**
     * Print-the-CLI-help-and-exit helper.
     */
    static private void printHelpAndExit() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(
            "java -jar kafka.consumer.hadoop.jar <properties_filename> [options]", 
            options 
        );
        System.exit(0);
    }
}

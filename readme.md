== Running the mirror out of the box ==

    java -cp /etc/kafka/:kafka-mirror-0.7.2.jar co.gridport.Kafka /etc/kafka/mirror.properties


== Wrapping the Mirror in a java program ==

    Properties properties = MyProgram.class.getResourceAsStream("mirror.properties");
    Mirror myMirror = new Mirror(properties);
    
    //EITHER - this will block and log stats every 10 seconds
    myMirror.run(10); 
    
    //OR -this will only start mirror executors in the background and myMirror.shutdown();
    myMirror.start(); 
    
== mirror.properties ==

    See src/test/resources/mirror.properties and run /src/test/java/TestMirror.java for example.

    - resolver section - contains configuration destination resolver
    - producer sectinos - contains configuration for destination producer
    - consumer(s) section - contains all consumers
    
    Resolver and Producer configuration are common to all consumers, but there can be 
    multiple consuemrs consuming from different clusters or sets of topics.
    
    
    

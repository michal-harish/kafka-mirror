== About the MirrorResolver interface ==

    MirrorResolver is the heart of the mirror decisions about where the incoming message
    should go and with what partition key. 

    All is done in a single method..

       public List<MirrorDestination> resolve(MessageAndMetadata<Message> metaMsg) { .. }

    ..which receives all messages from every configured consumer and is expected to
    return a list of MirrorDestination objects each of which is a pair of topic-hash
    while the hash is optional in which case null (and thus random partition) will be used.

    The hash is in fact expected to be an integer value rather than any kind of hash 
    so that the built-in partitioner can transparently use a simple hash % num_partitions. 

== Running the mirror out of the box ==

    svn co http://xp-dev.com/svn/gridport.co/artifacts/kafka-mirror
    mvn package assembly:single    
    java -cp src/test/resources/:target/kafka-mirror-0.7.2.jar co.gridport.kafka.Mirror /etc/kafka/mirror.properties

    This will require /etc/kafka/mirror.properties to exist and properties as per example.
    The src/test/resources/: is added to the classpath to provide access to log4j.properties
    but may be replaced with custom ones. 

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
    - producer sectinos - contains the producer configuration for the destination cluster
    - consumer(s) section - contains all consumers which consume from the source clusters

    Resolver and Producer configuration are common to all consumers, but there can be 
    multiple consuemrs consuming from different clusters or sets of topics.


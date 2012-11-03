Kafka-Paritioned-Mirror
=======================

This Kafka Mirror implementation gives control over output(producer) partitioner which is not supported by the mirroring functionality bundled into the kafka broker 0.7.x. Mirroring single source message onto multiple destination topics is also supported. See extract from the readme file below for more details about the message-to-destination mapping implementation.

It is written in Java and can be run out of the box, provided the resolver.class is found on the classpath, or may be embedded in a Java/Scala program that contains the resolver.class implementation via maven dependency.

About the MirrorResolver interface
----------------------------------

MirrorResolver is the heart of the mirror decisions about where the incoming message
should go and with what partition key. 

All is done in a single method..

public List<MirrorDestination> resolve(MessageAndMetadata<Message> metaMsg) { .. }

..which receives all messages from every configured consumer and is expected to
return a list of MirrorDestination objects each of which is a pair of topic-hash
while the hash is optional in which case null (and thus random partition) will be used.

The hash is in fact expected to be an integer value rather than any kind of hash 
so that the built-in partitioner can transparently use a simple hash % num_partitions. 

Running the mirror out of the box
----------------------------------

git clone git://github.com/michal-harish/kafka-mirror.git
mvn package assembly:single    
java -cp src/test/resources/:target/kafka-mirror-0.7.2.jar co.gridport.kafka.Mirror [/etc/kafka/mirror.properties]

This will require [/etc/kafka/mirror.properties] to exist and properties as per example.
The src/test/resources/: is added to the classpath to provide access to log4j.properties
but may be replaced with custom ones. 

Wrapping the Mirror in a java program
-------------------------------------

Properties properties = MyProgram.class.getResourceAsStream("mirror.properties");
Mirror myMirror = new Mirror(properties);

//EITHER - this will block and log stats every 10 seconds
myMirror.run(10); 

//OR -this will only start mirror executors in the background and myMirror.shutdown();
myMirror.start(); 

mirror.properties
-----------------

The following snippet is the content from [src/test/resources/mirror.properties].
It is used in the embedded test [src/test/java/TestMirror.java]
* The resolver section contains configuration destination resolver used by the single internal producer.
* Producer section appears only once and contains the producer configuration for the destination cluster
* There are multiple consumer(s) each for mirroring a particular remote cluster and its topics.

	resolver.class = TestMirror$ExampleEncoder

	producer.zk.connect = localhost:2181
	producer.zk.connectiontimeout.ms = 10000
	producer.producer.type = async
	producer.queue.time = 100
	producer.queue.size = 50
	producer.compression.codec = 1

	consumer.1.topics.whitelist = topic1,topic2
	consumer.1.zk.connect = aos3.gridport.co:2181
	consumer.1.groupid = example-mirror
	consumer.1.zk.connectiontimeout.ms = 10000
	consumer.1.backoff.increment.ms = 250
		
	consumer.2.zk.connect = aos1.gridport.co:2181
	consumer.2.groupid = example-mirror
	consumer.2.topics.whitelist = topic1,topic2   
	consumer.2.zk.connectiontimeout.ms = 3000
	consumer.2.backoff.increment.ms = 250


    
Backlog
=======

    * take net.imagini implementation out and use git on top of svn to share the common code  
    * src/test/java/TestMirrorPartitioner
    * src/test/java/TestMirrorResolver
    * src/test/java/TestMirrorDestination
    * decide on unit-testing the MirrorExecutor
        ** brute force would be to launch two instances of kafka within the test
        ** elegant way would be to split executor into two classes with an interface in between, 
           one for the kafka context and another for testable logic, then mock the context with
           serving complete messageAndMetadata objects.



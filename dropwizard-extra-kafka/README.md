# Drop Wizard Extra Kafka
This module allows you to use Kafka inside your application

## How to use
* Add a dependency

		<dependency>
			<group>com.datasift.dropwizard</group>
			<artifact>dropwizard-extra-kafka</artifact>
			<version>0.9.2-1</version>
		</dependency>


* In your application in order to consume Kafka Messages add the following fields into your Application Configuration 
(note you may have multiples of these in your application)
		
		@NotNull
		@Valid
		private KafkaConsumerFactory kafkaConsumerFactory = new KafkaConsumerFactory();
		
		@JsonProperty("kafka-consumer")
		public KafkaConsumerFactory getKafkaConsumerFactory() {
			return kafkaConsumerFactory;
		}
		
		@JsonProperty("kafka-consumer")
		public void setKafkaConsumerFactory(KafkaConsumerFactory kafkaConsumerFactory) {
			this.kafkaConsumerFactory = kafkaConsumerFactory;
		}

* In your application in order to produce Kafka Messages add the following fields into your Application Configuration

		@NotNull
		@Valid
		private KafkaProducerFactory kafkaProducerFactory = new KafkaProducerFactory();
		
		@JsonProperty("kafka-producer")
		public KafkaProducerFactory getKafkaProducerFactory() {
			return kafkaProducerFactory = kafkaProducerFactory;
		}
		
	    @JsonProperty("kafka-producer")
    	public void setKafkaProducerFactory(KafkaProducerFactory kafkaProducerFactory) {
        this.kafkaProducerFactory = kafkaProducerFactory;
    	}
		

## Kafka configuration in config.yaml
* Kafka producers configuration have several fields

Simple example

		kafka-producer:
			broker:
			- localhost:9092

Complex example

		kafka-producer:
			broker: 
			  - localhost:4321
			  - 192.168.10.12:123
			  - localhost
			  - 192.168.4.21
			acknowledgement: (NEVER, LEADER, ALL)
			requestTimeout: 1 second
			compression: (none, gzip, snappy)
			maxRetries: 3
			retryBackOff: 100 milliseconds
			metadataMaxAge: 40 seconds
			asyncBatchInterval: 10ms
			maxBlock: 10s
			

Name               | Default    | Description
-------------------|------------|------------
broker             | (none)     | A list of brokers we connect to
acknowledgement    | NEVER      | Controls when a produce request is consider completed.
requestTimeout     | 10 seconds | The amount of time the broker will wait trying to meet the request
compression        | none       | Compression codec for all data generated.  Valid values are none, gzip and snappy
maxRetries	      | 3          | How many times we will retry a failed send request.
retryBackOff       | 100 ms     | How long the producer waits before trying to see if a new leader has been elected.
metadataMaxAge     | 30s        | The period of time in milliseconds after which we force a refresh of metadata.
asyncBatchInterval | 0ms        | Linger ms
maxBlock           | 60s        | The configuration controls how long {@link KafkaProducer#send()} and {@link KafkaProducer#partitionsFor} will block.
maxRequestSize     | 1 MB       | 
batchSize          | 16384      | The producer will attempt to batch records together into fewer requests whenever multiple records are being sent.

* Kafka consumers configuration have several fields as well

Simple example

        kafka-consumer:
        	brokers:
        	- localhost:9092
        	group: test
        	
        	
Complex example

		kafka-consumer:
			brokers:
			- localhost:9092
			sessionTimeout: 5 minutes
			receiveBufferSize: 1 GB
			sendBufferSize: 1 KB
			autoCommit: false
			

        		
Name                     | Default    | Description
-------------------------|------------|------------
brokers                  | (none)     | A list of brokers we connect to
minFetch                 | 1 bytes    | Minimum amount of data the server should return for a fetch request.
group                    | (none)     | The consumer group used for this consumer
heartbeatInterval        | 3 seconds  | The expected time between heartbeats
maxPartitionFetch        | 1 MB       | The maximum amount of data partiton the server will return.
sessionTimeout           | 30 seconds | The timeout used to detect failures when using Kafka's group management facilities.
autoOffsetReset          | latest     | What to do when there is no initial offset in Kafka
connectionsMaxIdle       | 540 seconds| Close idle connections after the number of milliseconds
autoCommit               | true       | If true consumer's offset will be periodically committed.
receiveBufferSize        | 32 KB      | The size of the receive client side buffer.
sendBufferSize           | 128 KB     | The size of the send client side buffer.
autoCommitInterval       | 5 seconds  | Frequency that the consumer offsets are auto-committed if autoCommit is true
clientIdSuffix           | (Optional) | An id string to pass to the server when making requests
maxFetchWaitInterval     | 500 ms     | The maximum amount of time the server will block before answering the fetch request if there isn't sufficient data to immediately satisfy the requirement given by minFetch 
reconnectBackoffInterval | 50 ms      | The amount of time to wait before attempting to reconnect to a given host.
retryBackoffInterval     | 100 ms     | The amount of time to wait before attempting to retry a failed fetch request to a given topic partition.

* If you are using PollingProcessor for consuming records from Kafka you will need the following configuration

Simple example:

		polling-processor:
			topics:
				- foo
				- bar

Name                | Default   | Description
--------------------|-----------|------------
topics		          | (none)    | The list of topics used by this
pollTimeout         | (Godot)   | This is amount of time we wait for reading the consumer. Godot == Forever
autoCommit          | true      | If it automatically commits the responses
batchCount          | 0         | How many commits if not using auto commit
shutdownOnFatal     | false     | If we should shutdown the Application if we see a fatal error
shutdownGracePeriod | 5 seconds | Grace period for shuting down.
startDelay          | 2 seconds | How long we delay before starting to consume messages.

Note: Both the Consumer and the Polling Processor must use the **SAME** auto commit modes.




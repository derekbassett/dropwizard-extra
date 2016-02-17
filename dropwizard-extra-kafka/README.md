# Drop Wizard Extra Kafka
This module allows you to use Kafka inside your application

## How to use
* Add a dependency

		<dependency>
			<group>com.datasift.dropwizard</group>
			<artifact>dropwizard-extra-kafka</artifact>
			<version>0.9.2-1-SNAPSHOT</version>
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
* First the kafka producers configuration have several fields

		kafka-producer:
			broker: 
			  - localhost:4321
			  - 192.168.10.12:123
			  - localhost
			  - 192.168.4.21
			acknowledgement: (NEVER, LEADER, ALL)
			requestTimeout: 1 second
			async: false
			compression: (none, gzip, snappy)
			maxRetries: 3
			retryBackOff: 100 milliseconds
			

Name            | Default    | Description
----------------|------------|------------
broker          | (none)     | A list of brokers we connect to
acknowledgement | NEVER      | Controls when a produce request is consider completed.
requestTimeout  | 10 seconds | The amount of time the broker will wait trying to meet the request
async           | false      | Is the message sent asynchrously on a background thread.
compression     | none       | Compression codec for all data generated.  Valid values are none, gzip and snappy
maxRetries	   | 3          | How many times we will retry a failed send request.
retryBackOff    | 100 ms     | How long the producer waits before trying to see if a new leader has been elected.

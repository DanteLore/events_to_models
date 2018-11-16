# Events to Models

It's what Data Engineers do!

## Running a local Kafka

To get Kafka running, follow the steps here: https://kafka.apache.org/quickstart

You'll also need to edit the file `~/kafka_2.11-2.0.0/config/server.properties` and add these lines somewhere near the top of the file:

```port = 9092
   advertised.host.name = localhost```

* Start Zookeeper: `bin/zookeeper-server-start.sh config/zookeeper.properties``
* Start Kafka: `bin/kafka-server-start.sh config/server.properties`
* Create the 'test' topic: `bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test`
* Send 100 messages to the test topic: `seq 100 | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test`

## Example data

The beer data comes from: https://www.kaggle.com/nickhould/craft-cans#beers.csv
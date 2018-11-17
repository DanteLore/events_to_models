# Events to Models

It's what Data Engineers do!

## Using the Confluent Platform

KSQL is part of the Confluent Platform, as is the Confluent Schema Repository.  So, ideally use Confluent.  You can use the Kafka sources (see below) if you don't fancy Confluent, but there will be things missing.

Download the distribution from here (you'll need to enter an email address): https://www.confluent.io/download/

Download the Open Source Only version, to save being trapped forever in enterprise hell ;)

You'll also need Java 8, which may not be installed by default (skip this step if it is installed, obvs):
```
sudo add-apt-repository ppa:webupd8team/java
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys C2518248EEA14886
sudo apt update
sudo apt install oracle-java8-installer
```

Start confluent thusly:
```
tar -xzf confluent-5.0.1-2.11.tar.gz
cd confluent-5.0.1
bin/confluent start
```

To check it's up and running, go to the web client: http://localhost:9021/

You can use this to create topics etc.

## Running a local Kafka

To get Kafka running, follow the steps here: https://kafka.apache.org/quickstart

Download and unzip Kafka:

```
wget https://www-eu.apache.org/dist/kafka/2.0.0/kafka_2.11-2.0.0.tgz
tar -xzf kafka_2.11-2.0.0.tgz
cp kafka_2.11-2.0.0
```

You'll need to edit the file `config/server.properties` and add these lines somewhere near the top of the file:

```
port = 9092
advertised.host.name = localhost
```

Now start the Zookeeper and Kafka services:

```
bin/zookeeper-server-start.sh config/zookeeper.properties
bin/kafka-server-start.sh config/server.properties
```

Finally, create a 'test' topic and send 100 messages to it:

```
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test
seq 100 | bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test
```

## Example data

The beer data comes from: https://www.kaggle.com/nickhould/craft-cans#beers.csv

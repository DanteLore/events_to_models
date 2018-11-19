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

Place it in a dir called 'data' in the root of this project. I don't think it's OK to distribute the data via git!

Note that, because I am too lazy to parse CSV properly, I manually removed all the "" escaped string fields.  There are two beers and one brewery with names including commas.


## Some KSQL Queries

Once you've got the data loaded into the topic using the BeerProducer and sample data from above, you can start to explore some KSQL queries:

Use the KSQL client in `.../bin/ksql`.  To make the queries start from the top of the queue when they are run, execute the following when you start the KSQL client:

```
SET 'auto.offset.reset' = 'earliest';
```

### Streams

Create a stream over the raw beer topic so we can query the incoming data with KSQL:
```
ksql> create stream beer_stream with (kafka_topic='beers', value_format='avro');

 Message
----------------
 Stream created
----------------

ksql> describe beer_stream;

Name                 : BEER_STREAM
 Field      | Type
----------------------------------------
 ROWTIME    | BIGINT           (system)
 ROWKEY     | VARCHAR(STRING)  (system)
 ROW        | INTEGER
 ABV        | DOUBLE
 IBU        | DOUBLE
 ID         | INTEGER
 NAME       | VARCHAR(STRING)
 STYLE      | VARCHAR(STRING)
 BREWERY_ID | INTEGER
 OUNCES     | DOUBLE
----------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
```

Run some basic queries on the stream - first one selects all beers, second one selects very strong beers:

```
ksql> select * from beer_stream limit 5;
1542628134266 | null | 0 | 0.05 | null | 1436 | Pub Beer | American Pale Lager | 408 | 12.0
1542628134284 | null | 1 | 0.066 | null | 2265 | Devil's Cup | American Pale Ale (APA) | 177 | 12.0
1542628134287 | null | 2 | 0.071 | null | 2264 | Rise of the Phoenix | American IPA | 177 | 12.0
1542628134289 | null | 3 | 0.09 | null | 2263 | Sinister | American Double / Imperial IPA | 177 | 12.0
1542628134292 | null | 4 | 0.075 | null | 2262 | Sex and Candy | American IPA | 177 | 12.0
Limit Reached
Query terminated

ksql> select * from beer_stream where abv > 0.07 limit 5;
1542628134287 | null | 2 | 0.071 | null | 2264 | Rise of the Phoenix | American IPA | 177 | 12.0
1542628134289 | null | 3 | 0.09 | null | 2263 | Sinister | American Double / Imperial IPA | 177 | 12.0
1542628134292 | null | 4 | 0.075 | null | 2262 | Sex and Candy | American IPA | 177 | 12.0
1542628134295 | null | 5 | 0.077 | null | 2261 | Black Exodus | Oatmeal Stout | 177 | 12.0
1542628134305 | null | 9 | 0.086 | null | 2131 | Cone Crusher | American Double / Imperial IPA | 177 | 12.0
Limit Reached
Query terminated
```

You can make streams persistent, running them forever based on a KSQL query.  Create a stream for very strong beers:

```
ksql> create stream very_strong_beers as select * from beer_stream where abv >= 0.07;

 Message
----------------------------
 Stream created and running
----------------------------
```

This query will now run forever, pushing all beers stronger than 7% into a topic `very_strong_beers`.  This is just a standard Kafka topic, so you can pull from it externally as well as using it as the basis of more KSQL jiggery pokery.  Note that the default topic and field names will be capitalised though, so you'll need to check that in the consumer.

### Tables

Create a table over the beer topic so we can explore the beers...
```
CREATE TABLE beer_table WITH (KAFKA_TOPIC='beers', VALUE_FORMAT='avro', KEY='id');
```


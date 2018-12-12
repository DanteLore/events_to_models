# Events to Models

You can read more about this on my website:
* Part 1: (The problem: turning events into models)[http://logicalgenetics.com/data-engineering-in-real-time/]
* Part 2: (Events into models using Kafka and KSQL)[http://logicalgenetics.com/kafkas-beer-festival/]
* Part 3: (Aggregations and charting)[http://logicalgenetics.com/time-at-the-bar-chart/]
* Part 4: (Loading and processing data with Kafka Connect and Kafka Streams)[http://logicalgenetics.com/one-for-the-road/]

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

Note that the beer stream doesn't have a key (see nulls in 2nd column below):
```
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

ksql> select * from beer_stream limit 4;
1542628134266 | null | 0 | 0.05 | null | 1436 | Pub Beer | American Pale Lager | 408 | 12.0
1542628134284 | null | 1 | 0.066 | null | 2265 | Devil's Cup | American Pale Ale (APA) | 177 | 12.0
1542628134287 | null | 2 | 0.071 | null | 2264 | Rise of the Phoenix | American IPA | 177 | 12.0
1542628134289 | null | 3 | 0.09 | null | 2263 | Sinister | American Double / Imperial IPA | 177 | 12.0
```

We need to create a stream over the top with an added key - keys must be strings in KSQL at time of writing, thus the cast.

```
ksql> CREATE STREAM beer_stream_with_key
    WITH (KAFKA_TOPIC='beer_stream_with_key', VALUE_FORMAT='avro')
    AS SELECT CAST(id AS string) AS id, row, abv, ibu, name, style, brewery_id, ounces
    FROM beer_stream PARTITION BY ID;

 Message
----------------------------
 Stream created and running
----------------------------

ksql> select * from beer_stream_with_key limit 4;
1542634100215 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542634100241 | 2264 | 2264 | 2 | 0.071 | null | Rise of the Phoenix | American IPA | 177 | 12.0
1542634100263 | 2131 | 2131 | 9 | 0.086 | null | Cone Crusher | American Double / Imperial IPA | 177 | 12.0
1542634100272 | 1980 | 1980 | 13 | 0.085 | null | Troll Destroyer | Belgian IPA | 177 | 12.0
```

Create a table over the beer topic so we can explore the beers...
```
ksql> CREATE TABLE beer_table WITH (KAFKA_TOPIC='beer_stream_with_key', VALUE_FORMAT='avro', KEY='id');

 Message
---------------
 Table created
---------------
ksql> describe beer_table;

Name                 : BEER_TABLE
 Field      | Type
----------------------------------------
 ROWTIME    | BIGINT           (system)
 ROWKEY     | VARCHAR(STRING)  (system)
 ID         | VARCHAR(STRING)
 ROW        | INTEGER
 ABV        | DOUBLE
 IBU        | DOUBLE
 NAME       | VARCHAR(STRING)
 STYLE      | VARCHAR(STRING)
 BREWERY_ID | INTEGER
 OUNCES     | DOUBLE
----------------------------------------
For runtime statistics and query details run: DESCRIBE EXTENDED <Stream,Table>;
ksql> select * from beer_table limit 4;
1542634282337 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542634282366 | 2262 | 2262 | 4 | 0.075 | null | Sex and Candy | American IPA | 177 | 12.0
1542634282363 | 2263 | 2263 | 3 | 0.09 | null | Sinister | American Double / Imperial IPA | 177 | 12.0
1542634282369 | 2261 | 2261 | 5 | 0.077 | null | Black Exodus | Oatmeal Stout | 177 | 12.0
```

The difference between a STREAM and a TABLE is that a table will automatically collapse events down, returning only the latest.  After running the producer several times, this can be seen by running:

```
ksql> select * from beer_table where name = 'Pub Beer';
1542634282337 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
^CQuery terminated

ksql> select * from beer_stream_with_key where name = 'Pub Beer';
1542628134266 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542628516940 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542629007336 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542629014857 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542629062006 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542629446962 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542630134068 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542630969970 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542634100215 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
1542634282337 | 1436 | 1436 | 0 | 0.05 | null | Pub Beer | American Pale Lager | 408 | 12.0
^CQuery terminated
```

Just to prove the above point further, let's look for beers over 10% ABV: There's four of them.  10% is a stupid strength for a beer!

```
ksql> select id, name, abv from beer_table where abv > 0.1;
2564 | Lee Hill Series Vol. 4 - Manhattan Style Rye Ale | 0.10400000000000001
2685 | London Balling | 0.125
2621 | Csar | 0.12
2565 | Lee Hill Series Vol. 5 - Belgian Style Quadrupel Ale | 0.128
```

Now, change the ABV of 'Pub Beer' to 0.11 in the `beers.csv` data file and _reload the whole file_ with the BeerProducer.

This will send new events for every single beer into the topic (we could have just sent one for the changed row, but it would have meant more code changes).

Let's look for beers over 10% again:  Lo and behold, there's 5 this time!
curl -X DELETE http://localhost:8081/subjects/Kafka-value
```
ksql> select id, name, abv from beer_table where abv > 0.1;
1436 | Pub Beer | 0.11
2565 | Lee Hill Series Vol. 5 - Belgian Style Quadrupel Ale | 0.128
2685 | London Balling | 0.125
2621 | Csar | 0.12
2564 | Lee Hill Series Vol. 4 - Manhattan Style Rye Ale | 0.10400000000000001
```

So the table has magically squashed the event data down into current state data!  Very exciting.


# Setting up Kafka Connect

Here are the details on setting up *SpoolDir* the Kafka Connect source for CSV and JSON.  This is quite an involved process.  [See this article for details](https://www.confluent.io/blog/ksql-in-action-enriching-csv-events-with-data-from-rdbms-into-AWS/) though a few of the steps in th article needed some tweaking before I could make them work.

First, download and compile the sources.  I couldn't make the unit tests work. Googling led me to just disable them!

```
$ sudo apt install git mvn

$ git clone https://github.com/jcustenborder/kafka-connect-spooldir.git
$ cd kafka-connect-spooldir
$ mvn clean package -DskipTests
$ cd ..

$ cp -Rvf kafka-connect-spooldir/target confluent-5.0.1/share/java/kafka-connect-spooldir

$ confluent-5.0.1/bin/confluent stop
$ confluent-5.0.1/bin/confluent start
```

You can check that this worked by going to the control centre (http://localhost:9021), clicking `Management --> Kafka Connect --> Sources --> Add New` and you should see the SpoolDir sources listed in the `Connector Class` box.

Next, create this directory structure somewhere (I used my home dir).  SpoolDir will pick up new files from source, process them and move them to either finished or error.

```
$ find csv/
csv/
csv/finished
csv/error
csv/source
```

Next step is to create a schema, create a temporary config file, pointing at the directories you just created:

`~/spool-conf.tmp`:
```
input.path=/home/dan/csv/source
finished.path=/home/dan/csv/finished
error.path=/home/dan/csv/error
csv.first.row.as.header=true
```

And the schema creation tool - this will pipe the config to a file ~/spooldir.config.

```
$ ~/confluent-5.0.1/bin/kafka-run-class com.github.jcustenborder.kafka.connect.spooldir.SchemaGenerator -t csv -f ~/csv/source/breweries.csv -c ~/spool-conf.tmp -i row | sed 's/\\:/:/g'|sed 's/\"/\\\"/g' > ~/spooldir.config
```

This creates a schema, but not quite in JSON format.  You need to hack it to look like the following (note that this file is included in this repo [spooldir.config](kafka-connect/spooldir.config).

```
{
"name": "csv-source-breweries",
"config": {
    "value.schema": "{\"name\":\"com.github.jcustenborder.kafka.connect.model.Value\",\"type\":\"STRUCT\",\"isOptional\":false,\"fieldSchemas\":{\"row\":{\"type\":\"INT64\",\"isOptional\":false},\"name\":{\"type\":\"STRING\",\"isOptional\":false},\"city\":{\"type\":\"STRING\",\"isOptional\":true},\"state\":{\"type\":\"STRING\",\"isOptional\":true}}}",
    "error.path": "/home/dan/csv/error",
    "input.path": "/home/dan/csv/source",
    "key.schema": "{\"name\":\"com.github.jcustenborder.kafka.connect.model.Key\",\"type\":\"STRUCT\",\"isOptional\":false,\"fieldSchemas\":{\"row\":{\"type\":\"INT64\",\"isOptional\":false}}}",
    "finished.path": "/home/dan/csv/finished",
    "halt.on.error": "false",
    "topic": "breweries",
    "tasks.max": "1",
    "connector.class": "com.github.jcustenborder.kafka.connect.spooldir.SpoolDirCsvSourceConnector",
    "input.file.pattern": "^.*.csv$",
    "csv.first.row.as.header": true
  }
}
```

Post the config file to create the Kafka Connect Source:

```
curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" http://localhost:8083/connectors/ --data "@spooldir.config"
```

You can check if this is running by going to http://localhost:9021/management/connect/ where it should appear in the list with status `Running`.

Finally, move some data into the `source` directory and it should be processed immediately and moved to the `finished` folder:

```
$ cp data/breweries.csv ~/csv/source

$ find csv/
csv/
csv/finished
csv/finished/breweries.csv
csv/error
csv/source
```

If this doesn't work, you can run this command to see the Kafka Connect logs, where any error messages are recorded:

```
$ confluent-5.0.1/bin/confluent log connect
```

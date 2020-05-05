package com.logicalgenetics.streams

import java.time.Duration
import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.avro.KafkaAvroCaseClassSerdes
import com.logicalgenetics.model.{Score, Vote}
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object VoteAggregatorStream {
  private val inputTopic = "votes"
  private val beerScoresTopic = "beer_scores"

  private val properties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "vote_aggregator")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.servers)
    p
  }

  // SO MANY implicit serdes!!
  private val voteSerde: Serde[Vote] = {
    implicit val schema: Schema = AvroSchema[Vote]
    implicit val recordFormat: RecordFormat[Vote] = RecordFormat[Vote]
    KafkaAvroCaseClassSerdes(Config.schemaRegistry)
  }

  private val scoreSerde: Serde[Score] = {
    implicit val schema: Schema = AvroSchema[Score]
    implicit val recordFormat: RecordFormat[Score] = RecordFormat[Score]
    KafkaAvroCaseClassSerdes(Config.schemaRegistry)
  }

  implicit val consumedVotes: Consumed[String, Vote] = Consumed.`with`(Serdes.String, voteSerde)
  implicit val groupedVotes: Grouped[String, Vote] = Grouped.`with`(Serdes.String, voteSerde)
  implicit val groupedScores: Grouped[String, Score] = Grouped.`with`(Serdes.String, scoreSerde)
  implicit val producedScores: Produced[String, Score] = Produced.`with`(Serdes.String, scoreSerde)

  implicit val materialisedVotes : Materialized[String, Vote, ByteArrayKeyValueStore] =
    Materialized.as("votes_by_customer")
      .withKeySerde(Serdes.String)
      .withValueSerde(voteSerde)

  implicit val materialisedScores : Materialized[String, Score, ByteArrayKeyValueStore] =
    Materialized.as("scores_by_beer")
      .withKeySerde(Serdes.String)
      .withValueSerde(scoreSerde)

  private def constructStreams: StreamsBuilder = {
    val builder = new StreamsBuilder()

    // Get the incoming votes
    val votes = builder.stream[String, Vote](inputTopic)

    // Group and key the votes by customer and beer IDs
    val customerVotes = votes.map {
      (_, v) => (s"${v.beerId}/${v.customerId}", v)
    }

    // Group by key (beerId/customerId) - this will drop duplicate votes per customerId
    val votesByCustomerAndBeer = customerVotes
      .groupByKey
      .reduce((_, latest) => latest)

    // Map to Scores, group by beer ID and reduce to give scores by beer
    val beerScores = votesByCustomerAndBeer
      .mapValues(vote => Score(vote.beerId, vote.vote))
      .groupBy((_, score) => (score.beerId.toString, score))
      .reduce(
        (v1, v2) => v1 + v2,
        (v1, v2) => v1 - v2
      )

    // Write to outgoing topic
    beerScores.toStream.to(beerScoresTopic)

    // Done :)
    builder
  }

  def main(args: Array[String]): Unit = {
    // Call our method to construct the streams
    val builder: StreamsBuilder = constructStreams

    // Start the streams
    val streams: KafkaStreams = new KafkaStreams(builder.build(), properties)
    streams.cleanUp()
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }
}

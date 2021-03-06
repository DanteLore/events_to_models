package com.logicalgenetics.voting

import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.avro.KafkaAvroCaseClassSerdes
import com.logicalgenetics.voting.model.{Score, Vote}
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import org.apache.avro.Schema
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{Consumed, Grouped, Materialized, Produced}
import org.apache.kafka.streams.scala.kstream.KTable
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, StreamsBuilder}

object VoteAggregatorStreamBuilder {
  val inputTopic = "votes"
  val beerScoresTopic = "beer_scores"

  val streamProperties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "vote_aggregator")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.servers)
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000) // Limit buffering to increase chattiness for demo!
    p
  }

  def build(builder: StreamsBuilder, schemaRegistryClient: SchemaRegistryClient): Unit = {

    // SO MANY implicit serdes!!
    val voteSerde: Serde[Vote] = {
      implicit val schema: Schema = AvroSchema[Vote]
      implicit val recordFormat: RecordFormat[Vote] = RecordFormat[Vote]
      KafkaAvroCaseClassSerdes(Config.schemaRegistry, schemaRegistryClient)
    }

    val scoreSerde: Serde[Score] = {
      implicit val schema: Schema = AvroSchema[Score]
      implicit val recordFormat: RecordFormat[Score] = RecordFormat[Score]
      KafkaAvroCaseClassSerdes(Config.schemaRegistry, schemaRegistryClient)
    }

    implicit val consumedVotes: Consumed[String, Vote] = Consumed.`with`(Serdes.String, voteSerde)
    implicit val groupedVotes: Grouped[String, Vote] = Grouped.`with`(Serdes.String, voteSerde)
    implicit val groupedScores: Grouped[String, Score] = Grouped.`with`(Serdes.String, scoreSerde)
    implicit val producedScores: Produced[String, Score] = Produced.`with`(Serdes.String, scoreSerde)

    implicit val materialisedVotes: Materialized[String, Vote, ByteArrayKeyValueStore] =
      Materialized.as("votes_by_customer")
        .withKeySerde(Serdes.String)
        .withValueSerde(voteSerde)

    implicit val materialisedScores: Materialized[String, Score, ByteArrayKeyValueStore] =
      Materialized.as("scores_by_beer")
        .withKeySerde(Serdes.String)
        .withValueSerde(scoreSerde)

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
    val beerScores: KTable[String, Score] = votesByCustomerAndBeer
      .mapValues(vote => Score(vote.beerId, vote.vote))
      .groupBy((_, score) => (score.beerId.toString, score))
      .reduce(_ + _, _ - _)

    // Write to outgoing topic
    beerScores.toStream.to(beerScoresTopic)
  }
}

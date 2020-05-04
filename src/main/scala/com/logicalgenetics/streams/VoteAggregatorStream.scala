package com.logicalgenetics.streams

import java.time.Duration
import java.util
import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.avro.{KafkaAvroCaseClassSerdes, RawAvroCaseClassSerdes}
import com.logicalgenetics.model.Vote
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object VoteAggregatorStream {
  private val votes_topic = "votes"

  private val properties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "vote_aggregator")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.servers)
    p
  }

  private val stringSerde: Serde[String] = Serdes.String
  implicit val voteSchema: Schema = AvroSchema[Vote]
  implicit val voteRF: RecordFormat[Vote] = RecordFormat[Vote]
  private val voteSerdes: Serde[Vote] = RawAvroCaseClassSerdes()
  //private val voteSerdes: Serde[Vote] = AvroCaseClassSerdes(Config.schemaRegistry)

  private implicit val consumed: Consumed[String, Vote] = Consumed.`with`(stringSerde, voteSerdes)
  private implicit val produced: Produced[String, String] = Produced.`with`(stringSerde, stringSerde)

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    // Get the incoming votes
    val votes: KStream[String, Vote] = builder.stream[String, Vote](votes_topic)

    val customerVotes: KStream[String, String] = votes.map {
      (_, v) => {
        (s"${v.customerId}/${v.beerId}", s"Vote is ${v.vote}")
      }
    }

    customerVotes.to("dan_topic")

    // Start the streams
    val streams: KafkaStreams = new KafkaStreams(builder.build(), properties)
    streams.cleanUp()
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }

  def toCaseClass(in : GenericRecord) : Vote = {
    val format = RecordFormat[Vote]
    // record is a type that implements both GenericRecord and Specific Record
    val record = format.from(in)

    record
  }
}

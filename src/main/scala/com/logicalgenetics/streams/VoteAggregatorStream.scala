package com.logicalgenetics.streams

import java.io.ByteArrayInputStream
import java.time.Duration
import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.model.Vote
import com.sksamuel.avro4s.{AvroInputStream, AvroSchema, BinaryFormat}
import com.sksamuel.avro4s.kafka.GenericSerde
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
  private val avroVoteSerde: Serde[Vote] = new GenericSerde[Vote](BinaryFormat)
  private val byteArraySerde: Serde[Array[Byte]] = new Serdes.ByteArraySerde

  private implicit val consumed: Consumed[String, Array[Byte]] = Consumed.`with`(stringSerde, byteArraySerde)
  private implicit val produced: Produced[String, String] = Produced.`with`(stringSerde, stringSerde)

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    // Get the incoming votes
    val votesBin: KStream[String, Array[Byte]] = builder.stream[String, Array[Byte]](votes_topic)

    val votes = votesBin.map((, v) => (k, deserializeAvro[Vote](v)))

    val customerVotes: KStream[String, String] = votes.map {
      // Every incoming record comes through the stream here, BUT...
      // all fields on value here are 0 in the outgoing string...
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

  def deserializeAvro[T](in: Array[Byte]): T = {
    val schema = AvroSchema[T]
    val bos = new ByteArrayInputStream(in)
    val is = AvroInputStream.data[T].from(bos).build(schema)
    val res = is.iterator.toList
    is.close()
    res.head
  }
}

package com.logicalgenetics.streams

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.time.Duration
import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.model.Vote
import com.sksamuel.avro4s.BinaryFormat
import com.sksamuel.avro4s.kafka.GenericSerde
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.util.Try

object VoteAggregatorStream {

  private val votes_topic = "votes"

  private val properties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "vote_aggregator")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.servers)
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, new GenericSerde[Vote](BinaryFormat))
    p
  }

  private val stringSerde: Serde[String] = Serdes.String
  private val avroVoteSerde: Serde[Vote] = new GenericSerde[Vote](BinaryFormat)

  private val peterSerde: Serde[Vote] = Serdes.fromFn(serialize[Vote] _, deserialize[Vote] _)

  private implicit val consumed: Consumed[String, Vote] = Consumed.`with`(stringSerde, avroVoteSerde)
  private implicit val produced: Produced[String, String] = Produced.`with`(stringSerde, stringSerde)

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    // Get the incoming votes
    val votes: KStream[String, Vote] = builder.stream[String, Vote](votes_topic)

    val customerVotes: KStream[String, String] = votes.map {
      // Every incoming record comes through the stream here, BUT...
      // all fields on value here are 0 in the outgoing string...
      (_, value) => (s"${value.customerId}/${value.beerId}", s"Vote is ${value.vote}")
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


  /**
   * Generic function to serialize a Java object of type T into an Array[Byte] for putting into Kafka.
   * @param data - the Java object to serialize
   * @tparam T - Type of the object to be serialized, e.g. a case class type
   * @return - the `data` object transformed to bytes
   */
  private def serialize[T >: Null](data: T): Array[Byte] = {
    val stream: ByteArrayOutputStream = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(stream)
    oos.writeObject(data)
    oos.close()
    stream.toByteArray
  }

  /**
   * Generic function to deserialize an Array[Byte] from Kafka into a Java object of type T.
   * @param data - Array[Byte] to deserialize into Java object of type T
   * @tparam T - Type of the object to deserialize into
   * @return an instance of type T
   */
  private def deserialize[T >: Null](data: Array[Byte]): Option[T] = {
    val objIn = new ObjectInputStream(new ByteArrayInputStream(data))
    val obj = Try(objIn.readObject().asInstanceOf[T])
    objIn.close()
    obj.toOption
  }
}

package com.logicalgenetics.streams

import java.time.Duration
import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.model.Vote
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import java.util
import collection.JavaConverters._

object RecordFormatSerdes {
  def recordFormatSerdes[T <: Product](implicit rf: RecordFormat[T]): Serde[T] = {
    Serdes.serdeFrom(
      new Avro4sSerializer(),
      new Avro4sDeserializer()
    )
  }
}

class Avro4sDeserializer[T <: Product](implicit rf: RecordFormat[T])  extends Deserializer[T] {
  private var inner = new GenericAvroDeserializer()

  override def deserialize(topic: String, data: Array[Byte]): T = {
    val bytes = inner.deserialize(topic, data)
    rf.from(bytes)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    inner.configure(configs, isKey)
  }

  override def close(): Unit = {
    inner.close()
  }
}

class Avro4sSerializer[T <: Product](implicit rf: RecordFormat[T])  extends Serializer[T] {
  private var inner = new GenericAvroSerializer()

  override def serialize(topic: String, data: T): Array[Byte] = {
    val gr = rf.to(data)
    inner.serialize(topic, gr)
  }

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    inner.configure(configs, isKey)
  }

  override def close(): Unit = {
    inner.close()
  }
}

object ImplicitSerdes {
  implicit val myTypeRecordFormat = RecordFormat[Vote]
  implicit val myTypeSerdes: Serde[Vote] = RecordFormatSerdes.recordFormatSerdes

  def configure(schemaRegistryUrl: String): Unit = {
    val config: util.Map[String, _] = Map("schema.registry.url" -> schemaRegistryUrl).asJava
    myTypeSerdes.configure(config, false)
  }
}

object VoteAggregatorStream {

  private val votes_topic = "votes"

  private val properties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "vote_aggregator")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.servers)
    p
  }

  private val stringSerde: Serde[String] = Serdes.String

  import ImplicitSerdes._
  ImplicitSerdes.configure("https://localhost:8081")

  private implicit val consumed: Consumed[String, Vote] = Consumed.`with`(stringSerde, myTypeSerdes)
  private implicit val produced: Produced[String, String] = Produced.`with`(stringSerde, stringSerde)

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    // Get the incoming votes
    //val votesBin: KStream[String, GenericRecord] = builder.stream[String, GenericRecord](votes_topic)
    val votes: KStream[String, Vote] = builder.stream[String, Vote](votes_topic)

    //val votes: KStream[String, Vote] = votesBin.map((k, v) => (k, toCaseClass(v)))

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

  def toCaseClass(in : GenericRecord) : Vote = {
    val format = RecordFormat[Vote]
    // record is a type that implements both GenericRecord and Specific Record
    val record = format.from(in)

    record
  }
}

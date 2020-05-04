package com.logicalgenetics.avro

import java.util

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.streams.serdes.avro.{GenericAvroDeserializer, GenericAvroSerializer}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

import scala.jdk.CollectionConverters._

object KafkaAvroCaseClassSerdes {
  private class Avro4sSerializer[T <: Product](implicit recordFormat: RecordFormat[T])  extends Serializer[T] {
    private val inner = new GenericAvroSerializer()

    override def serialize(topic: String, data: T): Array[Byte] = {
      val genericRecord = recordFormat.to(data)
      inner.serialize(topic, genericRecord)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      inner.configure(configs, isKey)
    }

    override def close(): Unit = {
      inner.close()
    }
  }

  private class Avro4sDeserializer[T <: Product](implicit recordFormat: RecordFormat[T])  extends Deserializer[T] {
    private val inner = new GenericAvroDeserializer()

    override def deserialize(topic: String, data: Array[Byte]): T = {
      val genericRecord = inner.deserialize(topic, data)
      recordFormat.from(genericRecord)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      inner.configure(configs, isKey)
    }

    override def close(): Unit = {
      inner.close()
    }
  }

  def apply[T <: Product](schemaRegUrl : String)(implicit rf: RecordFormat[T]): Serde[T] = {
    val serdes = Serdes.serdeFrom(
      new Avro4sSerializer(),
      new Avro4sDeserializer()
    )
    val config: util.Map[String, _] = Map("schema.registry.url" -> schemaRegUrl).asJava
    serdes.configure(config, false)
    serdes
  }
}

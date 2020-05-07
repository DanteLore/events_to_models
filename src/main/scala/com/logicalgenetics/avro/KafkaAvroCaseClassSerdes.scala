package com.logicalgenetics.avro

import java.util

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient
import io.confluent.kafka.serializers.{KafkaAvroDeserializer, KafkaAvroSerializer}
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

import scala.jdk.CollectionConverters._

object KafkaAvroCaseClassSerdes {
  private class Avro4sSerializer[T <: Product](schemaRegistryClient : SchemaRegistryClient)(implicit recordFormat: RecordFormat[T])  extends Serializer[T] {
    private val inner = new KafkaAvroSerializer(schemaRegistryClient)

    override def serialize(topic: String, data: T): Array[Byte] = {
      val record = recordFormat.to(data)
      inner.serialize(topic, record)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      inner.configure(configs, isKey)
    }

    override def close(): Unit = {
      inner.close()
    }
  }

  private class Avro4sDeserializer[T <: Product](schemaRegistryClient : SchemaRegistryClient)(implicit recordFormat: RecordFormat[T])  extends Deserializer[T] {
    private val inner = new KafkaAvroDeserializer(schemaRegistryClient)

    override def deserialize(topic: String, data: Array[Byte]): T = {
      val bytes = inner.deserialize(topic, data).asInstanceOf[GenericRecord]
      recordFormat.from(bytes)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
      inner.configure(configs, isKey)
    }

    override def close(): Unit = {
      inner.close()
    }
  }

  def apply[T <: Product](schemaRegUrl : String, schemaRegistryClient : SchemaRegistryClient)(implicit recordFormat: RecordFormat[T]): Serde[T] = {
    val serdes = Serdes.serdeFrom(
      new Avro4sSerializer(schemaRegistryClient),
      new Avro4sDeserializer(schemaRegistryClient)
    )
    val config: util.Map[String, _] = Map("schema.registry.url" -> schemaRegUrl).asJava
    serdes.configure(config, false)
    serdes
  }
}

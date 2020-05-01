package com.logicalgenetics.avro

import java.util

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.streams.serdes.avro.GenericAvroDeserializer
import org.apache.kafka.common.serialization.Deserializer

class Avro4sDeserializer[T <: Product](implicit rf: RecordFormat[T])  extends Deserializer[T] {
  private val inner = new GenericAvroDeserializer()

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

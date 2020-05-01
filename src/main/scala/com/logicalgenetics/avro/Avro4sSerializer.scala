package com.logicalgenetics.avro

import java.util

import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerializer
import org.apache.kafka.common.serialization.Serializer

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

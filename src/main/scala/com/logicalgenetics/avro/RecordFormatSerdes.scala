package com.logicalgenetics.avro

import com.sksamuel.avro4s.RecordFormat
import org.apache.kafka.common.serialization.{Serde, Serdes}

object RecordFormatSerdes {
  def recordFormatSerdes[T <: Product](implicit rf: RecordFormat[T]): Serde[T] = {
    Serdes.serdeFrom(
      new Avro4sSerializer(),
      new Avro4sDeserializer()
    )
  }
}

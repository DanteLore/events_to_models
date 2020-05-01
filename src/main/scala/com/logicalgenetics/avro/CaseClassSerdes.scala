package com.logicalgenetics.avro

import java.util
import com.sksamuel.avro4s.RecordFormat
import org.apache.kafka.common.serialization.{Serde, Serdes}
import scala.jdk.CollectionConverters._

object CaseClassSerdes {
  def serdeFor[T <: Product](implicit rf: RecordFormat[T]): Serde[T] = {
    val serdes = Serdes.serdeFrom(
      new Avro4sSerializer(),
      new Avro4sDeserializer()
    )
    val config: util.Map[String, _] = Map("schema.registry.url" -> "http://localhost:8081").asJava
    serdes.configure(config, false)
    serdes
  }
}

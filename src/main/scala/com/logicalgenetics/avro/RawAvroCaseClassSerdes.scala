package com.logicalgenetics.avro

import java.io.ByteArrayOutputStream
import java.util

import com.sksamuel.avro4s.RecordFormat
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericDatumReader, GenericDatumWriter, GenericRecord}
import org.apache.avro.io.{DecoderFactory, EncoderFactory}
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serdes, Serializer}

object RawAvroCaseClassSerdes {
  private class Avro4sSerializer[T <: Product](implicit recordFormat: RecordFormat[T], schema: Schema)  extends Serializer[T] {

    private val writer = new GenericDatumWriter[GenericRecord](schema)

    override def serialize(topic: String, data: T): Array[Byte] = {
      val genericRecord = recordFormat.to(data)

      val out = new ByteArrayOutputStream()
      val encoder = EncoderFactory.get.binaryEncoder(out, null)
      writer.write(genericRecord, encoder)
      encoder.flush()
      out.close()
      out.toByteArray
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

    }

    override def close(): Unit = {

    }
  }

  private class Avro4sDeserializer[T <: Product](implicit recordFormat: RecordFormat[T], schema: Schema)  extends Deserializer[T] {

    val reader = new GenericDatumReader[GenericRecord](schema)

    override def deserialize(topic: String, data: Array[Byte]): T = {
      val decoder = DecoderFactory.get.binaryDecoder(data.drop(5), null)
      val genericRecord = reader.read(null, decoder)

      recordFormat.from(genericRecord)
    }

    override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {

    }

    override def close(): Unit = {

    }
  }

  def apply[T <: Product]()(implicit rf: RecordFormat[T], schema: Schema): Serde[T] = {
    Serdes.serdeFrom(
      new Avro4sSerializer(),
      new Avro4sDeserializer()
    )
  }
}

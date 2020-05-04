package com.logicalgenetics.avro

import java.io.ByteArrayOutputStream

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
  }

  private class Avro4sDeserializer[T <: Product](implicit recordFormat: RecordFormat[T], schema: Schema)  extends Deserializer[T] {

    val reader = new GenericDatumReader[GenericRecord](schema)

    override def deserialize(topic: String, data: Array[Byte]): T = {

      // Skip 1 'magic' byte and an Int32 for the Schema number
      val payload = data.drop(5)

      val decoder = DecoderFactory.get.binaryDecoder(payload, null)
      val genericRecord = reader.read(null, decoder)

      recordFormat.from(genericRecord)
    }
  }

  def apply[T <: Product]()(implicit rf: RecordFormat[T], schema: Schema): Serde[T] = {
    Serdes.serdeFrom(
      new Avro4sSerializer(),
      new Avro4sDeserializer()
    )
  }
}

package com.logicalgenetics.streams

import java.util.concurrent.TimeUnit
import java.util.{Collections, Properties}

import com.logicalgenetics.Config
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object BreweryCsvProcessorStream extends App {

  lazy val schema: Schema = new Schema.Parser().parse("""
    {
      "namespace": "logicalgenetics.brewery",
      "type": "record",
      "name": "brewery",

      "fields": [
         {"name": "id",    "type": "string", "default": ""},
         {"name": "name",  "type": "string", "default": ""},
         {"name": "city",  "type": "string", "default": ""},
         {"name": "state", "type": "string", "default": ""}
      ]
    }""")

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "brewery_validator")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.servers)
    p
  }

  private def process(inputKey : String, inputValue: String) : Option[(String, GenericRecord)] = {
    val splits = inputValue.split(',').map(_.trim).map(_.toUpperCase)

    splits match {
      case Array(row, name, city, state) =>
        val breweryRecord: GenericRecord = new GenericData.Record(schema)
        breweryRecord.put("id", row)
        breweryRecord.put("name", name)
        breweryRecord.put("city", city)
        breweryRecord.put("state", state)

        Some((row, breweryRecord))

      case _ => None
    }
  }

  val stringSerde: Serde[String] = Serdes.String
  val genericAvroSerde: Serde[GenericRecord] = {
    val gas = new GenericAvroSerde
    gas.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.schemaRegistry), false)
    gas
  }
  implicit val produced: Produced[String, GenericRecord] = Produced.`with`(stringSerde, genericAvroSerde)
  implicit val consumed: Consumed[String, String] = Consumed.`with`(stringSerde, stringSerde)


  val builder = new StreamsBuilder()
  val textLines: KStream[String, String] = builder.stream[String, String]("raw-brewery-text")
  val records: KStream[String, GenericRecord] = textLines.flatMap((k, v) => process(k, v))
  records.to("brewery-rows-good")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()

  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(10, TimeUnit.SECONDS)
  }
}

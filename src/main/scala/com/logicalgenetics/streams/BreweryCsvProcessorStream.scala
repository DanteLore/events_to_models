package com.logicalgenetics.streams

import java.time.Duration
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

object BreweryCsvProcessorStream {

  lazy val schema: Schema = new Schema.Parser().parse("""
    {
      "namespace": "logicalgenetics.breweries",
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

  val stringSerde: Serde[String] = Serdes.String
  val genericAvroSerde: Serde[GenericRecord] = {
    val gas = new GenericAvroSerde
    gas.configure(Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.schemaRegistry), false)
    gas
  }
  implicit val consumed: Consumed[String, String] = Consumed.`with`(stringSerde, stringSerde)
  implicit val producedGood: Produced[String, GenericRecord] = Produced.`with`(stringSerde, genericAvroSerde)
  implicit val producedBad: Produced[String, String] = Produced.`with`(stringSerde, stringSerde)

  private def validate(inputValue: String) : Boolean = {

    inputValue.split(',').map(_.trim) match {
      // Skip the header row
      case Array("row", "name", "city", "state") => false

      // Any of the columns are empty
      case arr : Array[String] if arr.contains("") => false

      // row num is non-numeric
      case Array(row, _, _, _) if row.exists(!_.isDigit)  => false

      // State is an invalid length
      case Array(_, _, _, state) if state.length != 2 => false

      // Healthy row
      case Array(row, name, city, state) => true

      // Otherwise bad row (wrong number of cols etc)
      case _ => false
    }
  }

  private def toAvro(inputValue: String) : Option[(String, GenericRecord)] = {

    inputValue.split(',').map(_.trim) match {
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



  def main(args: Array[String]): Unit = {
    // Get the lines
    val builder = new StreamsBuilder()
    val textLines: KStream[String, String] = builder.stream[String, String]("raw_brewery_text")

    // Validate and branch
    val Array(good, bad) = textLines.branch(
      (_, v) => validate(v),
      (_, _) => true
    )

    // Send the good rows to the good topic
    good.flatMap((_, v) => toAvro(v)).to("breweries")

    // ...and the bad to the bad
    bad.to("brewery_rows_bad")

    // Start the streams
    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.cleanUp()
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }
}

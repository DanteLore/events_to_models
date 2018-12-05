import java.util.concurrent.TimeUnit

import com.logicalgenetics.Config
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.serialization.StringSerializer
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

object BreweryCsvProcessorStream extends App {

  import java.util.Properties

  import io.confluent.kafka.serializers.KafkaAvroSerializer
  import org.apache.avro.generic.GenericRecord
  import org.apache.kafka.streams.scala.ImplicitConversions._
  import org.apache.kafka.streams.scala.Serdes._

  lazy val schema: Schema = new Schema.Parser().parse("""
    {
      "namespace": "logicalgenetics.brewery",
      "type": "record",
      "name": "brewery",

      "fields": [
 |        {"name": "id",    "type": "string", "default": ""},
 |        {"name": "name",  "type": "string", "default": ""},
 |        {"name": "city",  "type": "string", "default": ""},
 |        {"name": "state", "type": "string", "default": ""}
      ]
    }""")

  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "brewery_validator")
    val bootstrapServers = if (args.length > 0) args(0) else Config.servers
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    p
  }

  lazy val producer : KafkaProducer[String, GenericRecord] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", Config.servers)
    properties.put("schema.registry.url", Config.schemaRegistry)
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[KafkaAvroSerializer])

    ???
    //new KafkaAvroSerializer[String, GenericRecord](properties)
  }

  private def process(inputKey : String, inputValue: String) : (String, GenericRecord) = {
    val splits = inputValue.split(',').map(_.trim).map(_.toUpperCase)


    val Array(row,name,city,state) = splits
    val breweryRecord: GenericRecord = new GenericData.Record(schema)
    breweryRecord.put("id", row)
    breweryRecord.put("name", name)
    breweryRecord.put("city", city)
    breweryRecord.put("state", state)

    (row, breweryRecord)
  }


  val builder = new StreamsBuilder()
  val textLines: KStream[String, String] = builder.stream[String, String]("raw-brewery-text")
  val records: KStream[String, GenericRecord] = textLines.map((k, v) => process(k, v))

  //records.to("brewery-rows-good")

  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
  streams.cleanUp()

  streams.start()

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(10, TimeUnit.SECONDS)
  }
}
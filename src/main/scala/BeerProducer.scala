import java.util.Properties

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import io.confluent.kafka.serializers.KafkaAvroSerializer

import scala.io.Source

object BeerProducer {

  val beerFile = "data/beers.csv"
  val topic = "beers"

  lazy val schema: Schema = new Schema.Parser().parse("""
    {
      "namespace": "logicalgenetics.beer",
      "type": "record",
      "name": "beer",

      "fields": [
        {"name": "row",        "type": "int"},
        {"name": "abv",        "type": ["double", "null"]},
        {"name": "ibu",        "type": ["double", "null"]},
        {"name": "id",         "type": "int"},
        {"name": "name",       "type": "string"},
        {"name": "style",      "type": "string"},
        {"name": "brewery_id", "type": "int"},
        {"name": "ounces",     "type": "double"}
      ]
    }""")

  lazy val producer : KafkaProducer[String, GenericRecord] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("schema.registry.url", "http://localhost:8081")
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[KafkaAvroSerializer])

    new KafkaProducer[String, GenericRecord](properties)
  }

  def createBeerFrom(line : String) : GenericRecord = {
    val Array(row,abv,ibu,id,name,style,brewery_id,ounces) = line.split(',')
    val beer: GenericRecord = new GenericData.Record(schema)
    beer.put("row", row.toInt)
    beer.put("abv", abv match { case "" => null; case x => x.toDouble})
    beer.put("ibu", ibu match { case "" => null; case x => x.toDouble})
    beer.put("id", id.toInt)
    beer.put("name", name)
    beer.put("style", style)
    beer.put("brewery_id", brewery_id.toInt)
    beer.put("ounces", ounces.toDouble)

    beer
  }

  def main(args: Array[String]): Unit = {
    val bufferedSource = Source.fromFile(beerFile)
    for (line <- bufferedSource.getLines.drop(1)) {
      // The call to 'get' here forces us to be synchronous by waiting for the send to complete
      producer.send(new ProducerRecord[String, GenericRecord](topic, createBeerFrom(line))).get()
    }
    bufferedSource.close()
  }
}
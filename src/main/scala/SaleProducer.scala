import java.util.Properties
import scala.util.Random

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

object SaleProducer {

  val beerFile = "data/beers.csv"
  val topic = "sales"

  lazy val schema: Schema = new Schema.Parser().parse(
    """
    {
      "namespace": "logicalgenetics.sale",
      "type": "record",
      "name": "sale",

      "fields": [
         {"name": "beer_id", "type": "int"},
         {"name": "bar", "type": "int"},
         {"name": "price",   "type": "int"}
      ]
    }""")

  lazy val producer: KafkaProducer[String, GenericRecord] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("schema.registry.url", "http://localhost:8081")
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[KafkaAvroSerializer])

    new KafkaProducer[String, GenericRecord](properties)
  }

  lazy val beerIds: Array[String] = {
    val bufferedSource = Source.fromFile(beerFile)
    val ids = bufferedSource.getLines.drop(1).map(_.split(',')).map { case Array(_, _, _, id, _, _, _, _) => id }.toArray
    bufferedSource.close()
    ids
  }

  def createSale : GenericRecord = {
    val sale: GenericRecord = new GenericData.Record(schema)
    sale.put("beer_id", beerIds(Random.nextInt(beerIds.size)).toInt)
    sale.put("bar", Random.nextInt(4) + 1) // There are 4 bars (i.e. 4 cash registers)
    sale.put("price", if(Random.nextDouble() > 0.75) 2 else 1) // Beer festival; 1 token per half
    sale
  }

  def main(args: Array[String]): Unit = {

    while (true) {
      producer.send(new ProducerRecord[String, GenericRecord](topic, createSale)).get()

      Thread.sleep(1000)
    }
  }
}
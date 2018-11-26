package com.logicalgenetics.beer

import java.util.Properties

import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import scala.util.Random

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
         {"name": "beer_id", "type": "int", "default": 0},
         {"name": "bar",     "type": "int", "default": 0},
         {"name": "price",   "type": "int", "default": 0}
      ]
    }""")

  lazy val producer: KafkaProducer[String, GenericRecord] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.56.101:9092")
    properties.put("schema.registry.url", "http://192.168.56.101:8081")
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
    // There are 4 bars (i.e. 4 cash registers) - some more popular than others!
    sale.put("bar", Random.nextInt(10) match {
      case x if 0 to 3 contains x => 1
      case x if 4 to 6 contains x => 2
      case x if 7 to 8 contains x => 3
      case _ => 4
    })
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

package com.logicalgenetics.beer

import java.lang.Double
import java.time.Duration
import java.util

import com.logicalgenetics.Config
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object BeerConsumer {

  val topic = "beers"

  lazy val consumer: KafkaConsumer[String, GenericRecord] = {
    val properties = new util.Properties()
    properties.put("bootstrap.servers", Config.servers)
    properties.put("schema.registry.url", Config.schemaRegistry)
    properties.put("group.id", "cheese-group99")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[KafkaAvroDeserializer])
    properties.put("auto.offset.reset", "earliest")

    new KafkaConsumer[String, GenericRecord](properties)
  }

  def fetchBeers: List[ConsumerRecord[String, GenericRecord]] = {
    Iterator.continually(consumer.poll(Duration.ofSeconds(5)))
      .takeWhile(_.count() > 0)
      .flatMap(_.iterator().asScala)
      .toList
  }

  def main(args: Array[String]): Unit = {
    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(0) // get a partition assigned
    consumer.seekToBeginning(consumer.assignment())

    var i = 0
    val records = fetchBeers

    for (record <- records) {
      val beer = record.value()
      val name = beer.get("name").toString
      val abv = beer.get("abv") match {
        case null => "unknown"
        case x: Double => s"${x * 100}%"
      }
      println(s"$name $abv")
    }

    println(s"Read ${records.length}")
  }
}

package com.logicalgenetics.reports

import java.lang.Double
import java.time.Duration
import java.util

import com.logicalgenetics.Config
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.json4s.{DefaultFormats, Formats}
import org.scalatra._
import org.scalatra.json._

import scala.collection.JavaConverters._
import scala.collection.mutable


case class BreweryRow(name : String, city : String, state : String, sales : Int)


object BreweryCache {
  lazy val breweries : mutable.Map[String, BreweryRow] = mutable.Map[String, BreweryRow]()

  def isEmpty : Boolean = breweries.isEmpty

  def update(items: Seq[BreweryRow]): Seq[BreweryRow] = {
    breweries ++= items.map(b => b.name -> b)
    breweries.values.toList
  }
}


class BreweryServer extends ScalatraServlet with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  lazy val consumer: KafkaConsumer[String, GenericRecord] = {
    val properties = new util.Properties()
    properties.put("bootstrap.servers", Config.servers)
    properties.put("schema.registry.url", Config.schemaRegistry)
    properties.put("group.id", "cheese-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[KafkaAvroDeserializer])
    properties.put("auto.offset.reset", "earliest")

    new KafkaConsumer[String, GenericRecord](properties)
  }

  before() {
    contentType = formats("json")
  }

  def fetchBreweries: List[ConsumerRecord[String, GenericRecord]] = {
    Iterator.continually(consumer.poll(Duration.ofSeconds(1)))
      .takeWhile(_.count() > 0)
      .flatMap(_.iterator().asScala)
      .toList
  }

  get("/") {
    consumer.subscribe(util.Arrays.asList("brewery_league_table"))

    if(BeerCache.isEmpty) {
      consumer.poll(0) // get a partition assigned
      consumer.seekToBeginning(consumer.assignment())
    }

    val updates = fetchBreweries
      .map(record => record.value())
      .map { beer =>
        BreweryRow(
          beer.get("BREWERY").toString,
          beer.get("CITY").toString,
          beer.get("STATE").toString,
          beer.get("SALES").toString.toInt
        )
      }

    val breweries = BreweryCache.update(updates)

    Map("count" -> breweries.length, "processed" -> updates.length, "breweries" -> breweries)
  }
}
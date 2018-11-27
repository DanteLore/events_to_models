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


case class BeerRow(name : String, abv : String, sales : Int)


object BeerCache {
  lazy val beers : mutable.Map[String, BeerRow] = mutable.Map[String, BeerRow]()

  def isEmpty : Boolean = beers.isEmpty

  def update(bars: Seq[BeerRow]): Seq[BeerRow] = {
    beers ++= bars.map(b => b.name -> b)
    beers.values.toList
  }
}


class BeerServer extends ScalatraServlet with JacksonJsonSupport {

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

  def fetchBeers: List[ConsumerRecord[String, GenericRecord]] = {
    Iterator.continually(consumer.poll(Duration.ofSeconds(1)))
      .takeWhile(_.count() > 0)
      .flatMap(_.iterator().asScala)
      .toList
  }

  get("/") {
    consumer.subscribe(util.Arrays.asList("beer_league_table"))

    if(BeerCache.isEmpty) {
      consumer.poll(0) // get a partition assigned
      consumer.seekToBeginning(consumer.assignment())
    }

    val updates = fetchBeers
      .map(record => record.value())
      .map { beer =>
        BeerRow(
          beer.get("NAME").toString,
          beer.get("ABV") match {
            case null => "unknown"
            case x: Double => f"${x * 100}%.1f"
          },
          beer.get("SALES").toString.toInt // !
        )
      }

    val beers = BeerCache.update(updates)

    Map("count" -> beers.length, "processed" -> updates.length, "beers" -> beers)
  }
}
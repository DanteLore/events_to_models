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


class ReportServer extends ScalatraFilter with JacksonJsonSupport {

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
    Iterator.continually(consumer.poll(Duration.ofSeconds(10)))
      .takeWhile(_.count() > 0)
      .flatMap(_.iterator().asScala)
      .toList
  }

  get("/beers") {
    consumer.subscribe(util.Arrays.asList("beers"))
    consumer.poll(0) // get a partition assigned
    consumer.seekToBeginning(consumer.assignment())

    val beers = fetchBeers
      .map(record => record.value())
      .map { beer =>
        Map(
          "name" -> beer.get("name").toString,
          "abv" -> {
            beer.get("abv") match {
              case null => "unknown"
              case x: Double => s"${x * 100}%"
            }
          }
        )
      }

    Map("count" -> beers.length, "beers" -> beers)
  }
}
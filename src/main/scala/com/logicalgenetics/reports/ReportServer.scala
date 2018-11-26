package com.logicalgenetics.reports

import org.scalatra._
import org.scalatra.json._
import org.json4s.{DefaultFormats, Formats}
import org.scalatra.json._

import java.lang.Double
import java.time.Duration
import java.util

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.generic.GenericRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._


class ReportServer extends ScalatraFilter with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  lazy val consumer: KafkaConsumer[String, GenericRecord] = {
    val properties = new util.Properties()
    properties.put("bootstrap.servers", "192.168.56.101:9092")
    properties.put("schema.registry.url", "http://192.168.56.101:8081")
    properties.put("group.id", "cheese-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[KafkaAvroDeserializer])
    properties.put("auto.offset.reset", "earliest")

    new KafkaConsumer[String, GenericRecord](properties)
  }

  before() {
    contentType = formats("json")
  }

  get("/sales") {
    consumer.subscribe(util.Arrays.asList("beers"))

    val records = consumer.poll(Duration.ofSeconds(5))

    ("Read" -> records.count())
/*      {for (record <- records.iterator().asScala) {
      val beer = record.value()
      val name = beer.get("name").toString
      val abv = beer.get("abv") match {
        case null => "unknown"
        case x: Double => s"${x * 100}%"
      }
      <li>{name} &nbsp; {abv}</li>
    }}</ul>*/
  }
}
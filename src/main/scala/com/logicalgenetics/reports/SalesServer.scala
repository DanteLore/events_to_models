package com.logicalgenetics.reports

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


object SalesCache {
  lazy val sales : mutable.Map[String, String] = mutable.Map[String, String]()

  def update(bars: Seq[(String, String)]): Seq[(String, String)] = {
    sales ++= bars.toMap
    sales.toList
  }
}


class SalesServer extends ScalatraServlet with JacksonJsonSupport {

  protected implicit lazy val jsonFormats: Formats = DefaultFormats

  lazy val consumer: KafkaConsumer[String, GenericRecord] = {
    val properties = new util.Properties()
    properties.put("bootstrap.servers", Config.servers)
    properties.put("schema.registry.url", Config.schemaRegistry)
    properties.put("group.id", "takings.by.bar")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[KafkaAvroDeserializer])
    properties.put("auto.offset.reset", "latest")

    new KafkaConsumer[String, GenericRecord](properties)
  }

  before() {
    contentType = formats("json")
  }

  def fetchRows: List[ConsumerRecord[String, GenericRecord]] = {
    Iterator.continually(consumer.poll(Duration.ofSeconds(1)))
      .takeWhile(_.count() > 0)
      .flatMap(_.iterator().asScala)
      .toList
  }

  get("/") {
    consumer.subscribe(util.Arrays.asList("takings_by_bar_last_min"))

    val rows = fetchRows

    val updates = rows
      .map(record => record.value())
      .map(x => (x.get("BAR").toString, x.get("SALES").toString))

    val bars = SalesCache
      .update(updates)
      .map {case (bar, sales) => Map("bar" -> bar.toInt, "sales" -> sales.toInt)}

    Map("count" -> bars.length, "processed" -> rows.length, "bars" -> bars)
  }
}
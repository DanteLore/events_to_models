package com.logicalgenetics.examples

import java.time.{Duration, Instant}
import java.util.Properties

import com.logicalgenetics.Config
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.scala.{ByteArrayKeyValueStore, Serdes, StreamsBuilder}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import scala.jdk.CollectionConverters._

class GenericKafkaStreamsExamples extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {

  private val streamProperties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "examples")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.servers)
    p
  }
  private val recordBaseTime = Instant.parse("2020-01-01T10:00:00Z")
  private val advance1Min = Duration.ofMinutes(1)

  it should "pass some numbers through as-is" in {
    val builder = new StreamsBuilder

    implicit val consumed: Consumed[String, Int] = Consumed.`with`(Serdes.String, Serdes.Integer)
    implicit val produced: Produced[String, Int] = Produced.`with`(Serdes.String, Serdes.Integer)

    val numbers = builder.stream[String, Int]("input")
    numbers.to("output")

    val driver = new TopologyTestDriver(builder.build, streamProperties)
    val inputTopic = driver.createInputTopic("input", new StringSerializer(), new IntegerSerializer())
    val outputTopic = driver.createOutputTopic("output", new StringDeserializer(), new IntegerDeserializer())

    Seq(
      ("a", 1),
      ("b", 2),
      ("c", 3),
      ("d", 4)
    ) foreach { case (k, v) => inputTopic.pipeInput(k, v) }

    val result = outputTopic.readRecordsToList().asScala.toList

    result.map(_.value) shouldEqual Seq(1, 2, 3, 4)

    driver.close()
  }

  it should "do a simple aggregation" in {

    val builder = new StreamsBuilder

    implicit val keySerde: Serde[String] = Serdes.String
    implicit val valueSerde: Serde[Int] = Serdes.Integer

    implicit val consumed: Consumed[String, Int] = Consumed.`with`(Serdes.String, Serdes.Integer)
    implicit val produced: Produced[String, Int] = Produced.`with`(Serdes.String, Serdes.Integer)
    implicit val grouped: Grouped[String, Int] = Grouped.`with`(Serdes.String, Serdes.Integer)
    implicit val materialisedVotes: Materialized[String, Int, ByteArrayKeyValueStore] = Materialized.as("counts")

    val numbers = builder.stream[String, Int]("input")
    val counts = numbers.groupBy((k, _) => k).reduce((a, b) => a + b)
    counts.toStream.to("output")

    val driver = new TopologyTestDriver(builder.build, streamProperties)
    val inputTopic = driver.createInputTopic("input", new StringSerializer(), new IntegerSerializer())
    val outputTopic = driver.createOutputTopic("output", new StringDeserializer(), new IntegerDeserializer())

    Seq(
      ("a", 1),
      ("a", 2),
      ("b", 30),
      ("b", 40)
    ) foreach { case (k, v) => inputTopic.pipeInput(k, v) }

    val result = outputTopic.readRecordsToList().asScala.toList

    result.map(x => (x.key, x.value)) shouldEqual Seq(("a", 1), ("a", 3), ("b", 30), ("b", 70))

    driver.close()
  }
}

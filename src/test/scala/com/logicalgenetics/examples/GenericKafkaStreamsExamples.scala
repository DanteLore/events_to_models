package com.logicalgenetics.examples

import java.util.Properties

import com.logicalgenetics.Config
import org.apache.kafka.common.serialization._
import org.apache.kafka.streams.scala.kstream.{Consumed, Grouped, Joined, Materialized, Produced, StreamJoined}
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

  it should "do a simple filter" in {
    val builder = new StreamsBuilder

    implicit val consumed: Consumed[String, Int] = Consumed.`with`(Serdes.String, Serdes.Integer)
    implicit val produced: Produced[String, Int] = Produced.`with`(Serdes.String, Serdes.Integer)

    val numbers = builder.stream[String, Int]("input")
    val odds = numbers.filter((_, v) => v % 2 == 1)
    odds.to("output")

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

    result.map(_.value) shouldEqual Seq(1, 3)

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
    val counts = numbers.groupByKey.reduce((a, b) => a + b)
    counts.toStream.to("output")

    val driver = new TopologyTestDriver(builder.build, streamProperties)
    val inputTopic = driver.createInputTopic("input", new StringSerializer(), new IntegerSerializer())
    val outputTopic = driver.createOutputTopic("output", new StringDeserializer(), new IntegerDeserializer())

    Seq(
      ("a", 1),
      ("a", 1),
      ("b", 30),
      ("a", 2),
      ("b", 40)
    ) foreach { case (k, v) => inputTopic.pipeInput(k, v) }

    val result = outputTopic.readRecordsToList().asScala.toList

    result.map(x => (x.key, x.value)) shouldEqual Seq(("a", 1), ("a", 2), ("b", 30), ("a", 4), ("b", 70))

    driver.close()
  }

  it should "join a stream to a table" in {
    val builder = new StreamsBuilder

    implicit val consumedValues: Consumed[Int, Int] = Consumed.`with`(Serdes.Integer, Serdes.Integer)
    implicit val consumedReference: Consumed[Int, String] = Consumed.`with`(Serdes.Integer, Serdes.String)
    implicit val produced: Produced[Int, String] = Produced.`with`(Serdes.Integer, Serdes.String)
    implicit val joined: Joined[Int, Int, String] = Joined.`with`(Serdes.Integer, Serdes.Integer, Serdes.String)

    val referenceTable = builder.stream[Int, String]("reference_data").toTable
    val dataStream = builder.stream[Int, Int]("data")
    val both = dataStream.join(referenceTable)((count, name) => s"Sold $count $name")

    both.to("output")

    val driver = new TopologyTestDriver(builder.build, streamProperties)
    val dataTopic = driver.createInputTopic("data", new IntegerSerializer(), new IntegerSerializer())
    val referenceTopic = driver.createInputTopic("reference_data", new IntegerSerializer(), new StringSerializer())
    val outputTopic = driver.createOutputTopic("output", new IntegerDeserializer(), new StringDeserializer())

    Seq( // Fill the table first
      (1, "Cheese"),
      (2, "Beans"),
      (3, "Toast")
    ) foreach { case (k, v) => referenceTopic.pipeInput(k, v) }

    Seq( // Then the stream
      (1, 5),
      (1, 4),
      (2, 10)
    ) foreach { case (k, v) => dataTopic.pipeInput(k, v) }

    val result = outputTopic.readRecordsToList().asScala.toList

    result.map(_.value) shouldEqual Seq(
      "Sold 5 Cheese",
      "Sold 4 Cheese",
      "Sold 10 Beans"
    )

    driver.close()
  }
}

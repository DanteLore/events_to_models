package com.logicalgenetics.examples

import java.time.{Duration, Instant}
import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.voting.VoteAggregatorStreamBuilder
import org.apache.kafka.common.serialization.{IntegerDeserializer, IntegerSerializer, StringDeserializer, StringSerializer}
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.scala.{Serdes, StreamsBuilder}
import org.apache.kafka.streams.{StreamsConfig, TopologyTestDriver}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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

    { // This is where the magic happens
      val numbers = builder.stream[String, Int]("input")
      numbers.to("output")
    }

    val driver = new TopologyTestDriver(builder.build, streamProperties)
    val inputTopic = driver.createInputTopic("input", new StringSerializer(), new IntegerSerializer(), recordBaseTime, advance1Min)
    val outputTopic = driver.createOutputTopic("output", new StringDeserializer(), new IntegerDeserializer())

    Seq(
      ("", 1),
      ("", 2),
      ("", 3),
      ("", 4)
    ) foreach { case (k, v) => inputTopic.pipeInput(k, v) }

    val result = outputTopic.readRecordsToList().asScala.toList

    result.map(_.value) shouldEqual Seq(1, 2, 3, 4)

    driver.close()
  }
}

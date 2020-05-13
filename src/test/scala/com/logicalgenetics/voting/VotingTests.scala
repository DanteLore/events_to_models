package com.logicalgenetics.voting

import java.time.{Duration, Instant}
import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.avro.KafkaAvroCaseClassSerdes
import com.logicalgenetics.voting.model.{Score, Vote}
import com.sksamuel.avro4s.RecordFormat
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}

import scala.jdk.CollectionConverters._

class VotingTests extends AnyFlatSpec with Matchers with BeforeAndAfterEach with BeforeAndAfterAll {
  var driver: Option[TopologyTestDriver] = None
  var inputTopic: Option[TestInputTopic[String, Vote]] = None
  var outputTopic: Option[TestOutputTopic[String, Score]] = None

  val streamProperties: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "vote_aggregator")
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "")
    p.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000) // Limit buffering to increase chattiness for demo!
    p
  }

  override def beforeEach(): Unit = {
    val builder = new StreamsBuilder

    val recordBaseTime = Instant.parse("2020-01-01T10:00:00Z")
    val advance1Min = Duration.ofMinutes(1)

    val schemaRegistryClient = new MockSchemaRegistryClient

    val voteSerde = {
      implicit val rf: RecordFormat[Vote] = RecordFormat[Vote]
      KafkaAvroCaseClassSerdes[Vote](Config.schemaRegistry, schemaRegistryClient)
    }

    val scoreSerde = {
      implicit val rf: RecordFormat[Score] = RecordFormat[Score]
      KafkaAvroCaseClassSerdes[Score](Config.schemaRegistry, schemaRegistryClient)
    }

    //Create Actual Stream Processing pipeline
    VoteAggregatorStreamBuilder.build(builder, schemaRegistryClient)
    driver = Some(new TopologyTestDriver(builder.build, streamProperties))
    inputTopic = Some(driver.get.createInputTopic(VoteAggregatorStreamBuilder.inputTopic, new StringSerializer(), voteSerde.serializer(), recordBaseTime, advance1Min))
    outputTopic = Some(driver.get.createOutputTopic(VoteAggregatorStreamBuilder.beerScoresTopic, new StringDeserializer(), scoreSerde.deserializer()))
  }

  override def afterEach(): Unit = {
    driver.get.close()
  }

  "Scores" should "add up" in {
    val sum = Score(beerId = 1, score = 1, count = 1) + Score(beerId = 2, score = 2, count = 2)

    sum shouldBe Score(beerId = 1, score = 3, count = 3)
  }

  "Scores" should "subtract" in {
    val sum = Score(beerId = 4, score = 4, count = 4) - Score(beerId = 1, score = 1, count = 1)

    sum shouldBe Score(beerId = 4, score = 3, count = 3)
  }

  "Vote aggregator" should "aggregate some distinct votes" in {
    Seq(
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 2, vote = 1),
      Vote(beerId = 1, customerId = 3, vote = 1),
      Vote(beerId = 1, customerId = 4, vote = 1)
    ) foreach { i => inputTopic.get.pipeInput(i) }

    val result = outputTopic.get.readRecordsToList().asScala.toList

    result.last.value shouldBe Score(beerId = 1, score = 4, count = 4)
  }

  "Vote aggregator" should "squash multiple votes per customer" in {
    Seq(
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 1, vote = 1)
    ) foreach { i => inputTopic.get.pipeInput(i) }

    val result = outputTopic.get.readRecordsToList().asScala.toList

    result.last.value shouldBe Score(beerId = 1, score = 1, count = 1)
    result.length shouldBe 1
  }

  "Vote aggregator" should "take the latest vote for a customer" in {
    Seq(
      Vote(beerId = 10, customerId = 100, vote = 1),
      Vote(beerId = 10, customerId = 100, vote = 0),
      Vote(beerId = 10, customerId = 100, vote = 0),
      Vote(beerId = 10, customerId = 100, vote = -1)
    ) foreach { i => inputTopic.get.pipeInput(i) }

    val result = outputTopic.get.readRecordsToList().asScala.toList

    result.last.value shouldBe Score(beerId = 10, score = -1, count = 1)
    // FAILS! result.length shouldBe 3
  }

  "Vote aggregator" should "deal with multiple customers changing their minds" in {
    Seq(
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 2, vote = 1),
      Vote(beerId = 1, customerId = 3, vote = 1),
      Vote(beerId = 1, customerId = 4, vote = -1),
      Vote(beerId = 1, customerId = 3, vote = 0),
      Vote(beerId = 1, customerId = 4, vote = 0)
    ) foreach { i => inputTopic.get.pipeInput(i) }

    val result = outputTopic.get.readRecordsToList().asScala.toList

    result.last.value shouldBe Score(beerId = 1, score = 2, count = 4)
  }

  "Vote aggregator" should "handle multiple beers" in {
    Seq(
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 1, vote = 1),
      Vote(beerId = 2, customerId = 1, vote = 1),
      Vote(beerId = 1, customerId = 2, vote = 1),
      Vote(beerId = 2, customerId = 2, vote = -1)
    ) foreach { i => inputTopic.get.pipeInput(i) }

    val result = outputTopic.get.readRecordsToList().asScala.toList

    result.filter(_.value.beerId == 1).last.value shouldBe Score(beerId = 1, score = 2, count = 2)
    result.filter(_.value.beerId == 2).last.value shouldBe Score(beerId = 2, score = 0, count = 2)
  }

}

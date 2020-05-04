package com.logicalgenetics.beer

import java.util.Properties

import com.logicalgenetics.Config
import com.logicalgenetics.model.Vote
import com.sksamuel.avro4s.{AvroSchema, RecordFormat}
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source
import scala.util.Random

object VoteProducer {

  val beerFile = "data/beers.csv"
  val topic = "votes"

  lazy val schema: Schema = AvroSchema[Vote]

  lazy val producer: KafkaProducer[String, GenericRecord] = {
    val properties = new Properties()
    properties.put("bootstrap.servers", Config.servers)
    properties.put("schema.registry.url", Config.schemaRegistry)
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[KafkaAvroSerializer])

    new KafkaProducer[String, GenericRecord](properties)
  }

  lazy val beerIds: Array[String] = {
    val bufferedSource = Source.fromFile(beerFile)
    val ids = bufferedSource.getLines.drop(1).map(_.split(',')).map { case Array(_, _, _, id, _, _, _, _) => id }.toArray
    bufferedSource.close()
    ids
  }

  def createVote : Vote = {
    Vote(
      beerIds(Random.nextInt(10)).toInt, // Limit to first 10 beers to demonstrate dup votes more quickly
      Random.nextInt(10),
      Random.nextInt(9) match {
        case x if 0 to 3 contains x => 1
        case x if 4 to 7 contains x => -1
        case _ => 0
      }
    )
  }

  def main(args: Array[String]): Unit = {
    println(schema)

    while (true) {
      val vote = createVote
      val format = RecordFormat[Vote]
      producer.send(new ProducerRecord[String, GenericRecord](topic, format.to(vote))).get()

      Thread.sleep(1000)
    }
  }
}

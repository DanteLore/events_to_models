import java.time.Duration

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object Consumer {
  def main(args: Array[String]): Unit = {


    val properties = new util.Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "cheese-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[StringSerializer])

    println("Connecting")
    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe(util.Arrays.asList("test"))
    println("Connected")

    val producer = new KafkaProducer[Integer, String](properties)

    var i = 0
    while (true) {
      println("Sending")
      producer.send(new ProducerRecord[Integer, String]("test", "Hlowrld"))
      println("Sent")

      println("Reading")
      val records = kafkaConsumer.poll(Duration.ofSeconds(1))
      println(s"Read ${records.count()}")

      for (record <- records.iterator().asScala) {
        println("\n\n Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
      }
    }
  }
}
import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import scala.collection.JavaConverters._

object Consumer {
  def main(args: Array[String]): Unit = {
    val properties = new util.Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "cheese-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])

    val kafkaConsumer = new KafkaConsumer[String, String](properties)
    kafkaConsumer.subscribe(util.Arrays.asList("test"))

    var i = 0
    while (true) {
      val records = kafkaConsumer.poll(Duration.ofSeconds(5))
      println(s"Read ${records.count()}")

      for (record <- records.iterator().asScala) {
        println("\n\n Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
      }
    }
  }
}
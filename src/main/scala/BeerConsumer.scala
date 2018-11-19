import java.time.Duration
import java.util

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._

object BeerConsumer {

  lazy val consumer: KafkaConsumer[String, String] = {

    val properties = new util.Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "cheese-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])
    properties.put("auto.offset.reset", "earliest")

    new KafkaConsumer[String, String](properties)
  }

  def main(args: Array[String]): Unit = {
    consumer.subscribe(util.Arrays.asList("test"))

    var i = 0
    while (true) {
      val records = consumer.poll(Duration.ofSeconds(5))
      println(s"Read ${records.count()}")

      for (record <- records.iterator().asScala) {
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
      }
    }
  }
}
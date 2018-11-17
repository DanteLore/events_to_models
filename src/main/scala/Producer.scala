import java.util

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

object Producer {
  def main(args: Array[String]): Unit = {
    val properties = new util.Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("key.serializer", classOf[StringSerializer])
    properties.put("value.serializer", classOf[StringSerializer])

    properties.put("group.id", "cheese-group")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[StringDeserializer])

    val producer = new KafkaProducer[String, String](properties)

    (1 to 10).foreach(i => {
      // The call to 'get' here forces us to be synchronous by waiting for the send to complete
      producer.send(new ProducerRecord[String, String]("test", s"Hello $i")).get()
    })
  }
}
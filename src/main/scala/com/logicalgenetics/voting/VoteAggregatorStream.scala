package com.logicalgenetics.voting

import java.time.Duration
import com.logicalgenetics.Config
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.scala.StreamsBuilder

object VoteAggregatorStream {

  def main(args: Array[String]): Unit = {
    val builder = new StreamsBuilder()

    val schemaRegistryClient = new CachedSchemaRegistryClient(Config.schemaRegistry, Config.schemaRegCacheSize)

    // Call our method to construct the streams
    VoteAggregatorStreamBuilder.build(builder, schemaRegistryClient)

    // Start the streams
    val streams: KafkaStreams = new KafkaStreams(builder.build(), VoteAggregatorStreamBuilder.streamProperties)
    streams.cleanUp()
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.ShutdownHookThread {
      streams.close(Duration.ofSeconds(10))
    }
  }
}

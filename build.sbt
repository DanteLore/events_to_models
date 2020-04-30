name := "events_to_models"

version := "0.2"

scalaVersion := "2.13.1"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

def kafkaV = "2.5.0"

libraryDependencies ++= {
  Seq(
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.apache.kafka" % "kafka-streams" % kafkaV,
    "org.apache.kafka" % "kafka-clients" % kafkaV,
    "org.apache.kafka" %% "kafka-streams-scala" % kafkaV,
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2",
//    "org.slf4j" % "slf4j-api" % "1.7.1",
    "org.apache.avro" % "avro" % "1.8.2",
    "io.confluent" % "kafka-avro-serializer" % "5.5.0",
    "io.confluent" % "kafka-streams-avro-serde" % "5.5.0",
    "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts Artifact("javax.ws.rs-api", "jar", "jar"),
    "org.apache.kafka" %% "kafka-streams-scala" % kafkaV,
    "org.scalatra" %% "scalatra" % "2.7.0",
    "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106",
    "org.eclipse.jetty" % "jetty-server" % "8.1.8.v20121106",
    "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided",
    "org.scalatra" %% "scalatra-json" % "2.7.0",
    "org.json4s" %% "json4s-jackson" % "3.6.7",
    "com.softwaremill.sttp" %% "core" % "1.7.2"
  )
}
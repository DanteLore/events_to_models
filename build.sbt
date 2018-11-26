name := "events_to_models"

version := "0.1"

scalaVersion := "2.12.7"

resolvers += "confluent" at "https://packages.confluent.io/maven/"

libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.0.0",
    "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
    "ch.qos.logback" % "logback-classic" % "1.0.3",
    "ch.qos.logback" % "logback-core" % "1.0.3",
    "org.slf4j" % "slf4j-api" % "1.7.1",
    "org.apache.avro" % "avro" % "1.8.2",
    "io.confluent" % "kafka-avro-serializer" % "4.0.0",
    "javax.ws.rs" % "javax.ws.rs-api" % "2.1" artifacts (Artifact("javax.ws.rs-api", "jar", "jar")),
    "org.apache.kafka" %% "kafka-streams-scala" % "2.0.1",
    "org.scalatra" %% "scalatra" % "2.6.+",
    "org.eclipse.jetty" % "jetty-webapp" % "8.1.8.v20121106",
    "org.eclipse.jetty" % "jetty-server" % "8.1.8.v20121106",
    "javax.servlet" % "javax.servlet-api" % "3.1.0" % "provided"
  )
}
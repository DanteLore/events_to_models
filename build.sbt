name := "events_to_models"

version := "0.1"

scalaVersion := "2.12.7"

libraryDependencies ++= {
  Seq(
    "org.apache.kafka" %% "kafka" % "2.0.0",
    "org.apache.kafka" % "kafka-clients" % "0.10.0.0",
    "org.slf4j" % "slf4j-api" % "1.7.1",
    "org.slf4j" % "log4j-over-slf4j" % "1.7.1",  // for any java classes looking for this
    "ch.qos.logback" % "logback-classic" % "1.0.3"
  )
}

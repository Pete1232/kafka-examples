name := "kafka-examples"

version := "0.1"

scalaVersion := "2.12.6"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "1.1.1",
  "ch.qos.logback" % "logback-classic" % "1.3.0-alpha4"
)

fork in run := true

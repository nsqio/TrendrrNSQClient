organization := "com.github.dustismo"

name := "trendrr-nsq-client"

version := "1.1-SNAPSHOT"

libraryDependencies ++= Seq(
  "ch.qos.logback" % "logback-classic" % "1.0.13",
  "ch.qos.logback" % "logback-core" % "1.0.13",
  "io.netty" % "netty" % "3.6.6.Final",
  "org.slf4j" % "slf4j-api" % "1.6.4"
)

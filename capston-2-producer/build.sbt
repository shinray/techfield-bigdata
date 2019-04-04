name := "capston-2-producer"

version := "0.1"

scalaVersion := "2.11.8"

lazy val kafkaVersion = "2.1.0"
lazy val sparkVersion = "2.3.0"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.20"// % Test

// https://mvnrepository.com/artifact/com.twitter/hbc-twitter4j
libraryDependencies += "com.twitter" % "hbc-twitter4j" % "2.2.0"
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion
name := "capston-2-consumer"

version := "0.1"

scalaVersion := "2.11.8"

lazy val kafkaVersion = "2.1.0"
lazy val sparkVersion = "2.3.0"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.20" % Test

// Spark
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % kafkaVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// hbase
resolvers += "hortonworks" at "http://repo.hortonworks.com/content/groups/public/"
libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"
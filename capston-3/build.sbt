name := "capston-3"

version := "0.1"

scalaVersion := "2.11.8"
lazy val sparkVersion = "2.3.0"
lazy val kafkaVersion = "2.1.0"

// Spark
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % kafkaVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// Cassandra
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion
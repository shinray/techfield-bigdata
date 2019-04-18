name := "finalproject-consumer2"

version := "0.1"

scalaVersion := "2.11.8"
lazy val sparkVersion = "2.3.0"
//https://stackoverflow.com/questions/50907437/sbt-test-error-java-lang-nosuchmethoderror-net-jpountz-lz4-lz4blockinputstream
lazy val excludeJpountz = ExclusionRule(organization = "net.jpountz.lz4", name = "lz4")

// Spark
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// Cassandra
// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion

// Elasticsearch
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch
libraryDependencies += "org.elasticsearch" % "elasticsearch" % "6.7.0"
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.7.0"
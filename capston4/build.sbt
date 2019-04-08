name := "capston4"

version := "0.1"

scalaVersion := "2.11.8"
lazy val sparkVersion = "2.3.0"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.20" % Test

// Spark
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// Elasticsearch
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch
libraryDependencies += "org.elasticsearch" % "elasticsearch" % "6.7.0"
// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch-spark-20
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark-20" % "6.7.0"

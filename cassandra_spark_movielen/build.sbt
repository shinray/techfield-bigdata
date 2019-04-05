name := "cassandra_spark_movielen"

version := "0.1"

scalaVersion := "2.11.8"
lazy val sparkVersion = "2.3.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

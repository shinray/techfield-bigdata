name := "capston-2"

version := "0.1"

scalaVersion := "2.11.8"
//scalaVersion := "2.10.7"
lazy val kafkaVersion = "2.1.0"
lazy val sparkVersion = "2.3.0"
//lazy val sparkVersion = "2.2.3"

//lazy val sparkVersion = "2.0.0"
//
//libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
//libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion

// https://mvnrepository.com/artifact/com.twitter/hbc-twitter4j
libraryDependencies += "com.twitter" % "hbc-twitter4j" % "2.2.0"

// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.20" % Test

//libraryDependencies ++= Seq(
////  "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
//  "org.apache.kafka" % "kafka-clients" % "0.10.0.1"
//    exclude("javax.jms", "jms")
//    exclude("com.sun.jdmk", "jmxtools")
//    exclude("com.sun.jmx", "jmxri")
//)

// https://mvnrepository.com/artifact/org.apache.kafka/kafka
libraryDependencies += "org.apache.kafka" %% "kafka" % kafkaVersion
// https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients
libraryDependencies += "org.apache.kafka" % "kafka-clients" % kafkaVersion


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % sparkVersion
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka-0-10
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % kafkaVersion

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// hbase
//resolvers += "hortonworks" at "http://repo.hortonworks.com/content/groups/public/"
//libraryDependencies += "com.hortonworks" % "shc-core" % "1.1.1-2.1-s_2.11"
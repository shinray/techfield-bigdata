name := "twitterProducer"

version := "0.1"

scalaVersion := "2.11.8"

// https://mvnrepository.com/artifact/com.twitter/hbc
libraryDependencies += "com.twitter" % "hbc-core" % "2.2.0" pomOnly()
libraryDependencies += "com.twitter" % "hbc-twitter4j" % "2.2.0" pomOnly()
libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "0.10.1.0"
    exclude("javax.jms", "jms")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("com.sun.jmx", "jmxri")
    exclude("org.slf4j", "slf4j-simple")
)
//libraryDependencies ++= Seq(
//  "org.apache.spark" %% "spark-core" % "1.5.2",
//  "org.apache.spark" %% "spark-sql" % "1.5.2",
//  "org.apache.spark" %% "spark-streaming" % "1.5.2",// % "provided",
////  "org.apache.spark" %% "spark-streaming-kafka" % "1.6.3",
//  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "1.5.2" excludeAll(
//    ExclusionRule(organization = "org.spark-project.spark", name = "unused"),
//    ExclusionRule(organization = "org.apache.spark", name = "spark-streaming"),
//    ExclusionRule(organization = "org.apache.hadoop")),
//  "org.apache.spark" %% "spark-streaming-twitter" % "1.5.2" //1.6.3
//)

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0"


// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming-kafka
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.6.0"
name := "amazonKinesis-consumer"

version := "0.1"

scalaVersion := "2.11.8"

// Make the bad warnings go away
// https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.20"// % Test
// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.534"
// https://mvnrepository.com/artifact/com.amazonaws/amazon-kinesis-client
libraryDependencies += "com.amazonaws" % "amazon-kinesis-client" % "1.10.0"
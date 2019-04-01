name := "scalatest"

version := "0.1"

scalaVersion := "2.11.8"

addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.5.2")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.apache.spark" %% "spark-sql" % "2.3.0"
)
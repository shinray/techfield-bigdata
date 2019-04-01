package consumer

import java.util.Properties

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, LocationStrategies}
//import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object twitterConsumer {
  val topicName = "kafkaConsumerSpark"
  val groupName = "kafkaConsumerGroup"

  val properties = new Properties()
  properties.put("bootstrap.servers","localhost:9092")
  properties.put("group.id",groupName)
  properties.put("key.deserializer", classOf[StringDeserializer])
  properties.put("value.deserializer",classOf[StringDeserializer])
  properties.put("enable.auto.commit",(false: java.lang.Boolean)) // Important: need to manually commit then

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "localhost:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupName,
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val spark = SparkSession.builder.master("local[*]").appName("kafkaConsumerSpark")
    .getOrCreate()

  import spark.implicits._

//  val sqlContext = spark.sqlContext
  import spark.sqlContext.implicits._

//  val sparkConf = spark.conf//new SparkConf().setAppName("kafkaConsumerSpark").setMaster("local[*]")
  val sparkConf = spark.sparkContext
  // all things stream
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint("checkpoint")
  val sc = ssc.sparkContext
  sc.setLogLevel("WARN")

  // needs to be in a collection format
  val topics = topicName.split(",").map(_.trim)

  // empty dataframe and schema and stuff.
  case class twitterRow(created_at: String, text: String, screen_name: String, follower_count: Integer, friends_count: Integer, location: String)
  val schema = StructType(List(
    StructField("created_at",StringType,true),
    StructField("text",StringType,true),
    StructField("user",StructType(List(
      StructField("screen_name",StringType,true)
    )),true),
    StructField("follower_count",LongType,true),
    StructField("friends_count",LongType,true),
    StructField("location",StringType,true)
  ))
  val outSchema = StructType(List(
    StructField("created_at",StringType,true),
    StructField("text",StringType,true),
    StructField("screen_name",StringType,true),
    StructField("follower_count",LongType,true),
    StructField("friends_count",LongType,true),
    StructField("location",StringType,true)
  ))
  var df = spark.createDataFrame(spark.sparkContext.emptyRDD[Row],outSchema)

  // function
  def quitConsumer(): Unit = {
    df.collect()
    df.coalesce(1).write.mode("overwrite").json("/home/shinray/capston1json")
    println("SAVEing ❤")
//    System.exit(0)
  }

  def main (args: Array[String]) : Unit = {
    var counter: Int = 0
//    val stream = KafkaUtils.createDirectStream(ssc, "localhost:2181", groupName, topics).map(_._2)
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))
//    val lines = stream.map(i => (i.key, i.value)).map(_._2)
//    lines.print()
//    val df = spark.read.json(Seq(lines.toString).toDS())
//    df.show()
      stream.foreachRDD(rdd => {
        if (counter % 5 == 0) quitConsumer()
        else if (counter > 120) {
          println("limit reached, stopping")
          quitConsumer()
          ssc.stop(true,true)
          println("goodbye ❤")
          System.exit(0)
        }
        counter += 1
        println(counter)
//        if (rdd.count() == 0)
//          println("--- empty RDD")

//        else
//          println("--- New RDD with " + rdd.getNumPartitions
//          + " partitions and " + rdd.count() + " records")
        rdd.foreach(record => {
          println(record.value())
          val tmp = spark.sparkContext.makeRDD(record.value() :: Nil)
          var testdf = spark.read.schema(schema).json(tmp)
          testdf = testdf
              .withColumn("screen_name", testdf.col("user.screen_name"))
              .drop("user")
          df = df.unionByName(testdf)
          df.show()
        })
      })

    ssc.start()
    ssc.awaitTermination()
  }
}

package kafkaConsumerSpark

import java.util
import java.util.Properties

//import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer

import org.apache.spark.streaming.kafka.KafkaUtils

import org.apache.spark.streaming.{StreamingContext,Seconds}
//import org.apache.spark.streaming._
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
//import org.apache.spark.streaming.kafka010.ConsumerStrategies._

object kafkaConsumerSpark {

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

//  val spark = SparkSession.builder
//    .master("local[*]").appName("kafkaConsumerSpark").getOrCreate()

  val sparkConf = new SparkConf().setAppName("kafkaConsumerSpark").setMaster("local[*]")

  // all things stream
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint("checkpoint")
  val sc = ssc.sparkContext
  sc.setLogLevel("WARN")

  // needs to be in a collection format
  val topics = topicName.split(",").map((_,4)).toMap

  def main(args: Array[String]) : Unit  = {
    val stream = KafkaUtils.createStream(ssc, "localhost:2181", groupName, topics).map(_._2)
    val lines = stream.map(i => i.toLowerCase)
    lines.print()
//    var consumer : KafkaConsumer[String, String] = null
//    val stream = KafkaUtils.createDirectStream[String, String](
//      ssc, PreferConsistent, Subscribe[String,String](topicName,properties)
//    )
//    val kafkaStream = KafkaUtils.createDirectStream[String, String](
//      ssc,
//      PreferConsistent,
//      ConsumerStrategies.Subscribe[String,String](Array(topicName),kafkaParams)
//    )

//    try {
//      consumer = new KafkaConsumer[String, String](properties)
//      consumer.subscribe(util.Arrays.asList(topicName))
//      while (true) {
//        val records: ConsumerRecords[String, String] = consumer.poll(100);
//        for (record <- records) {
////          println("")
//        }
//      }
//    } catch {
//      case e: Exception => {
//        println("consumer exception:")
//        e.printStackTrace()
//      }
//    } finally {
//      consumer.close() // always close no matter what
//    }

    ssc.start()
    ssc.awaitTermination()
  }
}

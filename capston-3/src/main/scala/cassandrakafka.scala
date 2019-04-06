import org.apache.spark.sql.{Row, SparkSession}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object cassandrakafka {

  val topicName = "twitterCapstone2"
  val groupName = "kafkaConsumerGroup"

  // needs to be in a collection format
  val topics = topicName.split(",").map(_.trim)

  val kafkaParams = Map[String, Object](
    //    "bootstrap.servers" -> "localhost:9092,localhost:9093",
    "bootstrap.servers" -> "sandbox.hortonworks.com:6667", // hortonworks
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupName
  )

  // all things stream

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("CassandraCapstone")
    .getOrCreate()
  val sparkConf = spark.sparkContext
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint("checkpoint")
  val sc = ssc.sparkContext
//  sc.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics,kafkaParams)
    )
  }
}

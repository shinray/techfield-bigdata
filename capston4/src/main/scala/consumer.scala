import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.elasticsearch.client.ElasticsearchClient
import org.elasticsearch.spark.sql._

object consumer {

  // DEFINE
  val topicName = "capstone4"
  val groupName = "kafkaConsumerGroup"
  val bootstrapServers = "localhost:9092" // local - single node
  val esServers = "localhost"
  val esPorts = "9200"

  // Topics need to be in a collection
  val topics = topicName.split(",").map(_.trim)

  // Twitter schema (?)
  val schema = StructType(List(
//    StructField("id",LongType,false), // Cassandra needs a primary key so might as well
    StructField("created_at",StringType,true),
    StructField("text",StringType,true),
    StructField("user",StructType(List(
      StructField("screen_name",StringType,true),
      StructField("followers_count",LongType,true),
      StructField("friends_count",LongType,true),
      StructField("location",StringType,true)
    )),true)
  ))

  // Lovely Kafka settings
  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupName
  )

  // Sparky
  val spark = SparkSession.builder
    .config("es.index.auto.create","true")
    .config("spark.es.nodes",esServers)
    .config("spark.es.port",esPorts)
//    .config("spark.es.nodes.wan.only","true") // Necessary for AWS
    .master("local[*]") // all available threads on local
    .appName("ELKConsumer")
    .getOrCreate()
  val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
  ssc.checkpoint("checkpoint")
  spark.sparkContext.setLogLevel("WARN") // Loglevel

  // Elk
//  val es = ElasticsearchClient

  // Main
  def main(args: Array[String]): Unit = {

    // STREAM UP
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics,kafkaParams)
    )

    // What to do when you receive stuff on stream.
    stream.foreachRDD(rdd => rdd.foreach(record => {
      // Read records (JSON formatted string) into RDD format.
      val instream = spark.sparkContext.makeRDD(record.value() :: Nil)
      var df = spark.read.schema(schema).json(instream) // This is deprecated but ¯\_(ツ)_/¯
      // Massage dataframe into format we like
      df = df
        .withColumn("screen_name",df.col("user.screen_name"))
        .withColumn("followers_count",df.col("user.followers_count"))
        .withColumn("friends_count",df.col("user.friends_count"))
        .withColumn("location",df.col("user.location"))
        .drop("user")
      // Fill in null values, filter out some unwanted data.
      df = df.na.fill(" ")
      // Look what I made!
      df.show()

      // We are Elastic
      df.saveToEs("capstone4/json") // elasticsearch index
    }))

    // Don't forget to actually start the stream.
    ssc.start()
    ssc.awaitTermination()
  }
}

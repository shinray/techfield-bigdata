import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object cassandrakafka {

  // Some nice variables.
  val topicName = "capstone3"
  val groupName = "kafkaConsumerGroup"
  val bootstrapServers = "localhost:9092" // single node
//  val bootstrapServers = "localhost:9092,localhost:9093" // double node
//  val bootstrapServers = "sandbox.hortonworks.com:6667" // Hortonworks sandbox node

  // needs to be in a collection format
  val topics = topicName.split(",").map(_.trim)

  val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupName
  )

  // Twitter schema (?)
  val schema = StructType(List(
    StructField("id",LongType,false), // Cassandra needs a primary key so might as well
    StructField("created_at",StringType,true),
    StructField("text",StringType,true),
    StructField("user",StructType(List(
      StructField("screen_name",StringType,true),
      StructField("followers_count",LongType,true),
      StructField("friends_count",LongType,true),
      StructField("location",StringType,true)
    )),true)
  ))

  // all things stream

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("CassandraCapstone")
    .getOrCreate()
  val sparkConf = spark.sparkContext
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint("checkpoint")
  val sc = ssc.sparkContext
  sc.setLogLevel("WARN") // Set Log level

  // Cassandra stuff. Only needed so I can try to programmatically create a table
  val connector = CassandraConnector.apply(spark.sparkContext.getConf)

  def main(args: Array[String]): Unit = {

    // Connect to Cassandra
    val session = connector.openSession()
    // Create keyspace
    session
      .execute("CREATE KEYSPACE IF NOT EXISTS capstone WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} AND durable_writes = true;")
    // Create table
    session.execute("CREATE TABLE IF NOT EXISTS capstone.capstone3 (" +
      "id bigint PRIMARY KEY," + // I like your id...I think I will TAKE IT!!
      "created_at text," +
      "text text," +
      "screen_name text," +
      "followers_count bigint," +
      "friends_count bigint," +
      "location text" +
      ")")

    // Create stream
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics,kafkaParams)
    )

    // For each RDD, do something.
    stream.foreachRDD(rdd =>
      // I'll be honest, I don't really understand why stream.foreachRDD occasionally sends empty RDDs.
      // However, if I then further do rdd.foreach, I run these operations only on valid, non-empty objects.
      rdd.foreach(record => {
        // Read records (JSON formatted string) into RDD format.
        val instream = spark.sparkContext.makeRDD(record.value() :: Nil)
        var df = spark.read.schema(schema).json(instream)
        // I want screen_name as its own column.
        df = df
            .withColumn("screen_name",df.col("user.screen_name"))
            .withColumn("followers_count",df.col("user.followers_count"))
            .withColumn("friends_count",df.col("user.friends_count"))
            .withColumn("location",df.col("user.location"))
            .drop("user")
        // Fill in null values, filter out some unwanted data.
        df = df.na.fill(" ")
        df.show()

        // All right, time to write to Cassandra.
        // Give Cassandra the dataframe
        df.write
          .format("org.apache.spark.sql.cassandra")
          .mode("append")
          .options(Map(
            "table" -> "capstone3",
            "keyspace" -> "capstone"
          ))
          .save()
      })
    )

    ssc.start()
    ssc.awaitTermination()
  }
}

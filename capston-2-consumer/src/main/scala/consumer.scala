import org.apache.hadoop.hbase.{HBaseConfiguration, HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory,Admin}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.execution.datasources.hbase.HBaseTableCatalog
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

object consumer {
  // HBase
//    val conf = HBaseConfiguration.create()
//    val connection = ConnectionFactory.createConnection(conf)
//    val admin = connection.getAdmin()

  val topicName = "twitterCapstone2"
  val groupName = "kafkaConsumerGroup"

  val kafkaParams = Map[String, Object](
    //    "bootstrap.servers" -> "localhost:9092,localhost:9093",
    "bootstrap.servers" -> "sandbox.hortonworks.com:6667", // hortonworks
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupName
    //    "enable.auto.commit" -> (false: java.lang.Boolean) // Important: need to manually commit then
//    "auto.offset.reset" -> "earliest"
  )

  val spark = SparkSession.builder.master("local[*]").appName("twitterCapstone")
    .getOrCreate()

  import spark.implicits._

  import spark.sqlContext.implicits._

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
    StructField("follower_count",StringType,true),
    StructField("friends_count",StringType,true),
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

  // hbase catalog
  def catalog = s"""{
         |"table":{"namespace":"default", "name":"twitterCapstone"},
         |"rowkey":"screen_name",
         |"columns":{
           |"screen_name":{"cf":"rowkey", "col":"screen_name", "type":"string"},
           |"created_at":{"cf":"tweet", "col":"created_at", "type":"string"},
           |"text":{"cf":"tweet", "col":"text", "type":"string"},
           |"follower_count":{"cf":"user", "col":"follower_count", "type":"string"},
           |"friends_count":{"cf":"user", "col":"friends_count", "type":"string"},
           |"location":{"cf":"user", "col":"location", "type":"string"}
         |}
       |}""".stripMargin

  // create table in hbase if does not exist
//  def createTableIfNotExists(): Unit = {
//    val tableName = "output"
//    if (!admin.isTableAvailable(TableName.valueOf(tableName))) {
//      println("-------------Creating new table-------------")
//      val tableDesc = new HTableDescriptor(tableName)
//      tableDesc.addFamily(new HColumnDescriptor("z1".getBytes()))
//      admin.createTable(tableDesc)
//    }else{
//      println("-------------Warning! Table already exists!-------------")
//    }
//  }

  // function
  def quitConsumer(): Unit = {
//    df.collect()
    //    df.coalesce(1).write.mode("overwrite").json("/home/shinray/capston1json")
    //    df.coalesce(1).write
    //        .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
    //        .format("org.apache.hadoop.hbase.spark")
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //        .save()
    println("SAVEing ❤")
  }
  def main (args: Array[String]) : Unit = {
    var counter: Int = 0
//    createTableIfNotExists()
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))

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

      rdd.foreach(record => {
//        println(record.value())
        val tmp = spark.sparkContext.makeRDD(record.value() :: Nil)
        var testdf = spark.read.schema(schema).json(tmp)
        testdf = testdf
          .withColumn("screen_name", testdf.col("user.screen_name"))
          .drop("user")
//        testdf.show()
        // Fill null values
        testdf = testdf.na.fill(" ")
        testdf.show()
        testdf.write
          .options(Map(HBaseTableCatalog.tableCatalog -> catalog, HBaseTableCatalog.newTable -> "5"))
          .format("org.apache.spark.sql.execution.datasources.hbase")
          .save()
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}

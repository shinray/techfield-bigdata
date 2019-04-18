import java.util.UUID

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions._ // UDF()
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.elasticsearch.spark.sql._ // elasticsearch

object consumer {

  // #define
  val topicName = "CryptoCompare"
  val groupName = "kafkaConsumerGroup"
  val bootstrapServers = "localhost:9092" // single node
  // ES
  val esServers = "localhost"
  val esPorts = "9200"

  // needs to be in a collection format
  val topics = topicName.split(",").map(_.trim)

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> bootstrapServers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupName
  )

  // Schemas
  val currencySchema = StructType(List(
    StructField("DOGE",DoubleType,true),
    StructField("USD",DoubleType,true),
    StructField("EUR",DoubleType,true),
    StructField("JPY",DoubleType,true),
    StructField("GBP",DoubleType,true),
    StructField("CAD",DoubleType,true)
  ))

  val schema = StructType(List(
    StructField("id",StringType,true),
    StructField("BTC",currencySchema,true),
    StructField("ETH",currencySchema,true),
    StructField("LTC",currencySchema,true),
    StructField("EOS",currencySchema,true),
    StructField("BCH",currencySchema,true),
    StructField("BNB",currencySchema,true),
    StructField("ZEC",currencySchema,true),
    StructField("ETC",currencySchema,true),
    StructField("DASH",currencySchema,true),
    StructField("MITH",currencySchema,true),
    StructField("ABBC",currencySchema,true),
    StructField("XPM",currencySchema,true),
    StructField("XRP",currencySchema,true),
    StructField("XMR",currencySchema,true)
  ))

  // Sparky
  val spark = SparkSession.builder
    .config("es.index.auto.create","true")
    .config("spark.es.nodes",esServers)
    .config("spark.es.port",esPorts)
    .master("local[*]")
    .appName("CryptoCompare")
    .getOrCreate()
  val sparkConf = spark.sparkContext
  val ssc = new StreamingContext(sparkConf, Seconds(1))
  ssc.checkpoint("checkpoint")
  val sc = ssc.sparkContext
  sc.setLogLevel("WARN") // Set Log level

  // Cassandra stuff. Only needed so I can try to programmatically create a table
  val connector = CassandraConnector.apply(spark.sparkContext.getConf)

  // Main
  def main(args: Array[String]): Unit = {

    // Connect to Cassandra
    val session = connector.openSession()
    // Create keyspace
    session.execute(
      "CREATE KEYSPACE IF NOT EXISTS finalproject WITH replication = {'class':'SimpleStrategy', 'replication_factor':1} AND durable_writes = true;"
    )
    // Create table if not exists
    session.execute(
      "CREATE TABLE IF NOT EXISTS finalproject.crypto (" +
        "id text PRIMARY KEY," +
        "btc_doge double," +
        "btc_usd double," +
        "btc_eur double," +
        "btc_jpy double," +
        "btc_gbp double," +
        "btc_cad double," +
        "eth_doge double," +
        "eth_usd double," +
        "eth_eur double," +
        "eth_jpy double," +
        "eth_gbp double," +
        "eth_cad double," +
        "ltc_doge double," +
        "ltc_usd double," +
        "ltc_eur double," +
        "ltc_jpy double," +
        "ltc_gbp double," +
        "ltc_cad double," +
        "eos_doge double," +
        "eos_usd double," +
        "eos_eur double," +
        "eos_jpy double," +
        "eos_gbp double," +
        "eos_cad double," +
        "bch_doge double," +
        "bch_usd double," +
        "bch_eur double," +
        "bch_jpy double," +
        "bch_gbp double," +
        "bch_cad double," +
        "bnb_doge double," +
        "bnb_usd double," +
        "bnb_eur double," +
        "bnb_jpy double," +
        "bnb_gbp double," +
        "bnb_cad double," +
        "zec_doge double," +
        "zec_usd double," +
        "zec_eur double," +
        "zec_jpy double," +
        "zec_gbp double," +
        "zec_cad double," +
        "etc_doge double," +
        "etc_usd double," +
        "etc_eur double," +
        "etc_jpy double," +
        "etc_gbp double," +
        "etc_cad double," +
        "dash_doge double," +
        "dash_usd double," +
        "dash_eur double," +
        "dash_jpy double," +
        "dash_gbp double," +
        "dash_cad double," +
        "mith_doge double," +
        "mith_usd double," +
        "mith_eur double," +
        "mith_jpy double," +
        "mith_gbp double," +
        "mith_cad double," +
        "abbc_doge double," +
        "abbc_usd double," +
        "abbc_eur double," +
        "abbc_jpy double," +
        "abbc_gbp double," +
        "abbc_cad double," +
        "xpm_doge double," +
        "xpm_usd double," +
        "xpm_eur double," +
        "xpm_jpy double," +
        "xpm_gbp double," +
        "xpm_cad double," +
        "xrp_doge double," +
        "xrp_usd double," +
        "xrp_eur double," +
        "xrp_jpy double," +
        "xrp_gbp double," +
        "xrp_cad double," +
        "xmr_doge double," +
        "xmr_usd double," +
        "xmr_eur double," +
        "xmr_jpy double," +
        "xmr_gbp double," +
        "xmr_cad double" +
        ")"
    )

    // User defined func for creating guid
    val druid = udf(() => UUID.randomUUID().toString)

    // Create stream
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String,String](topics,kafkaParams)
    )

    stream.foreachRDD(rdd =>
      rdd.foreach(record => {
        val instream = spark.sparkContext.makeRDD(record.value() :: Nil)
        var df = spark.read.schema(schema).json(instream)
        df = df
          .withColumn("id", coalesce(df("id"), druid())) // insert random UUID as primary key
          .withColumn("btc_doge",df("BTC.DOGE"))
          .withColumn("btc_usd",df("BTC.USD"))
          .withColumn("btc_eur",df("BTC.EUR"))
          .withColumn("btc_jpy",df("BTC.JPY"))
          .withColumn("btc_gbp",df("BTC.GBP"))
          .withColumn("btc_cad",df("BTC.CAD"))
          .drop("BTC")
        df = df
          .withColumn("eth_doge",df("ETH.DOGE"))
          .withColumn("eth_usd",df("ETH.USD"))
          .withColumn("eth_eur",df("ETH.EUR"))
          .withColumn("eth_jpy",df("ETH.JPY"))
          .withColumn("eth_gbp",df("ETH.GBP"))
          .withColumn("eth_cad",df("ETH.CAD"))
          .drop("ETH")
        df = df
          .withColumn("ltc_doge",df("LTC.DOGE"))
          .withColumn("ltc_usd",df("LTC.USD"))
          .withColumn("ltc_eur",df("LTC.EUR"))
          .withColumn("ltc_jpy",df("LTC.JPY"))
          .withColumn("ltc_gbp",df("LTC.GBP"))
          .withColumn("ltc_cad",df("LTC.CAD"))
          .drop("LTC")
        df = df
          .withColumn("eos_doge",df("EOS.DOGE"))
          .withColumn("eos_usd",df("EOS.USD"))
          .withColumn("eos_eur",df("EOS.EUR"))
          .withColumn("eos_jpy",df("EOS.JPY"))
          .withColumn("eos_gbp",df("EOS.GBP"))
          .withColumn("eos_cad",df("EOS.CAD"))
          .drop("EOS")
        df = df
          .withColumn("bch_doge",df("BCH.DOGE"))
          .withColumn("bch_usd",df("BCH.USD"))
          .withColumn("bch_eur",df("BCH.EUR"))
          .withColumn("bch_jpy",df("BCH.JPY"))
          .withColumn("bch_gbp",df("BCH.GBP"))
          .withColumn("bch_cad",df("BCH.CAD"))
          .drop("BCH")
        df = df
          .withColumn("bnb_doge",df("BNB.DOGE"))
          .withColumn("bnb_usd",df("BNB.USD"))
          .withColumn("bnb_eur",df("BNB.EUR"))
          .withColumn("bnb_jpy",df("BNB.JPY"))
          .withColumn("bnb_gbp",df("BNB.GBP"))
          .withColumn("bnb_cad",df("BNB.CAD"))
          .drop("BNB")
        df = df
          .withColumn("zec_doge",df("ZEC.DOGE"))
          .withColumn("zec_usd",df("ZEC.USD"))
          .withColumn("zec_eur",df("ZEC.EUR"))
          .withColumn("zec_jpy",df("ZEC.JPY"))
          .withColumn("zec_gbp",df("ZEC.GBP"))
          .withColumn("zec_cad",df("ZEC.CAD"))
          .drop("ZEC")
        df = df
          .withColumn("etc_doge",df("ETC.DOGE"))
          .withColumn("etc_usd",df("ETC.USD"))
          .withColumn("etc_eur",df("ETC.EUR"))
          .withColumn("etc_jpy",df("ETC.JPY"))
          .withColumn("etc_gbp",df("ETC.GBP"))
          .withColumn("etc_cad",df("ETC.CAD"))
          .drop("ETC")
        df = df
          .withColumn("dash_doge",df("DASH.DOGE"))
          .withColumn("dash_usd",df("DASH.USD"))
          .withColumn("dash_eur",df("DASH.EUR"))
          .withColumn("dash_jpy",df("DASH.JPY"))
          .withColumn("dash_gbp",df("DASH.GBP"))
          .withColumn("dash_cad",df("DASH.CAD"))
          .drop("DASH")
        df = df
          .withColumn("mith_doge",df("MITH.DOGE"))
          .withColumn("mith_usd",df("MITH.USD"))
          .withColumn("mith_eur",df("MITH.EUR"))
          .withColumn("mith_jpy",df("MITH.JPY"))
          .withColumn("mith_gbp",df("MITH.GBP"))
          .withColumn("mith_cad",df("MITH.CAD"))
          .drop("MITH")
        df = df
          .withColumn("abbc_doge",df("ABBC.DOGE"))
          .withColumn("abbc_usd",df("ABBC.USD"))
          .withColumn("abbc_eur",df("ABBC.EUR"))
          .withColumn("abbc_jpy",df("ABBC.JPY"))
          .withColumn("abbc_gbp",df("ABBC.GBP"))
          .withColumn("abbc_cad",df("ABBC.CAD"))
          .drop("ABBC")
        df = df
          .withColumn("xpm_doge",df("XPM.DOGE"))
          .withColumn("xpm_usd",df("XPM.USD"))
          .withColumn("xpm_eur",df("XPM.EUR"))
          .withColumn("xpm_jpy",df("XPM.JPY"))
          .withColumn("xpm_gbp",df("XPM.GBP"))
          .withColumn("xpm_cad",df("XPM.CAD"))
          .drop("XPM")
        df = df
          .withColumn("xrp_doge",df("XRP.DOGE"))
          .withColumn("xrp_usd",df("XRP.USD"))
          .withColumn("xrp_eur",df("XRP.EUR"))
          .withColumn("xrp_jpy",df("XRP.JPY"))
          .withColumn("xrp_gbp",df("XRP.GBP"))
          .withColumn("xrp_cad",df("XRP.CAD"))
          .drop("XRP")
        df = df
          .withColumn("xmr_doge",df("XMR.DOGE"))
          .withColumn("xmr_usd",df("XMR.USD"))
          .withColumn("xmr_eur",df("XMR.EUR"))
          .withColumn("xmr_jpy",df("XMR.JPY"))
          .withColumn("xmr_gbp",df("XMR.GBP"))
          .withColumn("xmr_cad",df("XMR.CAD"))
          .drop("XMR")
        df.show()

        // Write to Cassandra
        df.write
          .format("org.apache.spark.sql.cassandra")
          .mode("append")
          .options(Map(
            "table" -> "crypto",
            "keyspace" -> "finalproject"
          ))
          .save()

        // Send to Elastic
        df.saveToEs("crypto/json")
      })
    )

    // Start the stream.
    ssc.start()
    ssc.awaitTermination()
  }
}

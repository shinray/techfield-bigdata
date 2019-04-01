//package kafkaProducerSpark
//
//import java.util.Properties
//
//import kafkaConsumerSpark.kafkaConsumerSpark.spark
//import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.streaming.{Seconds, StreamingContext}
////import org.apache.spark.streaming.
//import twitter4j.auth.OAuthAuthorization
//import twitter4j.conf.ConfigurationBuilder
//import org.apache.spark.streaming.twitter.TwitterUtils
//
//object kafkaProducerSpark {
//
//  // twitter super sekrit keys shh don't tell
//  private val CONSUMER_KEY = "WXC50jvoWct4SzHLHHeHI7MzD"
//  private val CONSUMER_SECRET = "pDA0JMsoka4RUGnKzwOKJBWxNN98JopCvOc6ymom4u1Ac1DnyP"
//  // Access level: READ_ONLY
//  private val ACCESS_TOKEN = "1105476295713087488-W3nJ7Fmty1TekFcFOTWlYr5JB0lxXN"
//  private val ACCESS_SECRET = "dXW78QUoHa25WLHWPw2LryU9VCasuXirqIGQxTDzLV0Hc"
//
//  // Keywords to look for
//  private val keywords = Array("article13", "usavchi", "travel")
//
//  val spark = SparkSession.builder
//    .master("local").appName("kafkaConsumerSpark").getOrCreate()
//
//  def main(args: Array[String]): Unit = {
//    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
//    val cb = new ConfigurationBuilder
//    cb.setDebugEnabled(true).setOAuthConsumerKey(CONSUMER_KEY).setOAuthConsumerSecret(CONSUMER_SECRET)
//      .setOAuthAccessToken(ACCESS_TOKEN).setOAuthAccessTokenSecret(ACCESS_SECRET)
//    val auth = new OAuthAuthorization(cb.build())
//
//    val tweetStream = TwitterUtils.createStream(ssc, Some(auth))
//
//    val statuses = tweetStream.map(status => (status.getText))
//
//    statuses.foreachRDD { (rdd) =>
//      rdd.foreachPartition { i =>
//        val props = new Properties()
//        props.put("bootstrap.servers","localhost:9092")
//        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
//        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer")
//
//        val producer = new KafkaProducer[String, String](props)
//
//        i.foreach { j =>
//          val data = new ProducerRecord[String, String]("kafkaConsumerSpark", null, j.toString())
//          println(j.toString)
//          producer.send(data)
//        }
//
//        producer.flush()
//        producer.close()
//      }
//    }
//
//    ssc.start()
//    ssc.awaitTermination()
//  }
//}

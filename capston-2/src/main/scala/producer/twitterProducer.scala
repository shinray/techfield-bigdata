package producer

import java.util.Properties
import java.util.concurrent.LinkedBlockingQueue

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.{Client, Constants}
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object twitterProducer {
//  // Define
  val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093"
//  val BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094"
  val SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"
  val KAFKA_TOPIC = "twitterCapstone"

  // Objects
  val props = new Properties()
  val queue = new LinkedBlockingQueue[String](100000) // thicc
  val endpoint = new StatusesFilterEndpoint()
  val auth = new OAuth1(twitterkeys.CONSUMER_KEY,twitterkeys.CONSUMER_SECRET,twitterkeys.ACCESS_TOKEN,twitterkeys.ACCESS_SECRET)

  def setup() = {
    // Kafka producer settings
    props.put("bootstrap.servers",BOOTSTRAP_SERVERS)
    props.put("key.serializer",SERIALIZER_CLASS)
    props.put("value.serializer",SERIALIZER_CLASS)

    endpoint.trackTerms(Lists.newArrayList(
      "article13","earthbound","mother3","undertale","deltarune","cheekibreeki","apache","kafka","savethemanuals"))

  }

  def main (args: Array[String]) : Unit = {
    setup()
    // twitter client
    val client = new ClientBuilder().hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue)).build()
    client.connect()

    val producer = new KafkaProducer[String,String](props)

    var i = 0
    while (i < 1000) {
      val record = new ProducerRecord[String,String](KAFKA_TOPIC,queue.take())
      println(i + ": " + record.toString)
      producer.send(record)
      i += 1
    }

    producer.close()
    client.stop()
  }
}

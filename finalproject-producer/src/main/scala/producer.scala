import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object producer {

  @volatile var keepRunning = true

  val BOOTSTRAP_SERVERS = "localhost:9092"
  val SERIALIZER_CLASS = "org.apache.kafka.common.serialization.StringSerializer"
  val KAFKA_TOPIC = "CryptoCompare"
  val CRYPTO_FSYMS = Seq("BTC","ETH","LTC","EOS","BCH","BNB","ZEC","ETC","DASH","MITH","ABBC","XPM","XRP","XMR") // symbols of interest
  val CRYPTO_TSYMS = Seq("DOGE","USD","EUR","JPY","GBP","CAD") // symbols to convert to
  val CC_API_KEY = Constants.CRYPTOCOMPARE_API_KEY

  // Kafka producer settings
  val props = new Properties()
  props.put("bootstrap.servers",BOOTSTRAP_SERVERS)
  props.put("key.serializer",SERIALIZER_CLASS)
  props.put("value.serializer",SERIALIZER_CLASS)

  // HTTP GET to String
  def get(url: String, connectionTimeout: Int = 5000, readTimeout: Int = 5000, requestMethod: String = "GET") = {
    import java.net.{URL, HttpURLConnection}
    val connection = (new URL(url)).openConnection.asInstanceOf[HttpURLConnection]
    connection.setConnectTimeout(connectionTimeout)
    connection.setReadTimeout(readTimeout)
    connection.setRequestMethod(requestMethod)
    val inputStream = connection.getInputStream
    val content = io.Source.fromInputStream(inputStream).mkString
    if (inputStream != null) inputStream.close
    content
  }

  // simplistic url request builder
  def requestBuilder(fsyms: Seq[String], tsyms: Seq[String]): String = {
    val sb: StringBuilder = new StringBuilder
    sb ++= "https://min-api.cryptocompare.com/data/pricemulti?"

    sb ++= "fsyms="
    sb ++= fsyms.mkString(",")

    sb += '&'
    sb ++= "tsyms="
    sb ++= tsyms.mkString(",")

    sb += '&'
    sb ++= "api_key="
    sb ++= CC_API_KEY

    return sb.toString()
  }

  def main(args: Array[String]): Unit = {

    // Intercept Ctrl-c shutdown
    val mainThread = Thread.currentThread();
    Runtime.getRuntime.addShutdownHook(new Thread() {override def run = {
      println("Intercepted ctrl-c, shutting down. This may take up to 30 seconds.")
      keepRunning = false
      mainThread.join()
    }})

    val producer = new KafkaProducer[String,String](props)
    val requestURL = requestBuilder(CRYPTO_FSYMS,CRYPTO_TSYMS)

    println("request url: " + requestURL)

    // Main loop
    while (keepRunning) {
      try {
        val content = get(requestURL)
        val record = new ProducerRecord[String,String](KAFKA_TOPIC,content)
        println(record.toString)
        producer.send(record)
      } catch {
        case ioe: java.io.IOException =>  {
          println("IOException: " + ioe.printStackTrace())
        }
        case ste: java.net.SocketTimeoutException => {
          println("SocketTimeoutException: " + ste.printStackTrace())
        }
      }
      Thread.sleep(1000 * 30)
    }

    // Cleanup
    producer.close()
    println("goodbye ❤")
  }
}

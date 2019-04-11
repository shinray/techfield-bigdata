package producer

import java.nio.ByteBuffer
import java.util.concurrent.LinkedBlockingQueue

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.kinesis.AmazonKinesisClient
import com.amazonaws.services.kinesis.model.PutRecordRequest
import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.OAuth1
//import org.apache.spark.SparkConf
import org.apache.spark.streaming.Milliseconds

object producer {

  // DEFINE
  val appName = "TwitterForKinesis"
  val streamName = "wm2"
  val endpointUrl = "https://kinesis.us-east-2.amazonaws.com"

  // Twitter. Get the good stuff here.
  val queue = new LinkedBlockingQueue[String](100000)
  val endpoint = new StatusesFilterEndpoint()
  val auth = new OAuth1(twitterkeys.CONSUMER_KEY,twitterkeys.CONSUMER_SECRET,twitterkeys.ACCESS_TOKEN,twitterkeys.ACCESS_SECRET)

  def main(args: Array[String]): Unit = {

//    // Spark
//    val conf = new SparkConf().setAppName(appName)
//    conf.setIfMissing("spark.master", "local[*]")

    // AWS Credentials
    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
    require(credentials != null,
    "No AWS credentials found. See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")

    // Kinesis Client
//    val kinesisClient = createKinesisClient(aws.getString("access-key"), aws.getString("secret-key"))
    val kinesisClient = new AmazonKinesisClient(credentials)
    kinesisClient.setEndpoint(endpointUrl)
    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size
    val numStreams = numShards
    val batchInterval = Milliseconds(2000)
    val kinesisCheckpointInterval = batchInterval

    // Set twitter search terms
    endpoint.trackTerms(Lists.newArrayList("aws","deprecated","kinesis","scala","owlboy","celeste","hollowknight"))

    // Twitter client
    val client = new ClientBuilder().hosts(Constants.STREAM_HOST)
      .endpoint(endpoint)
      .authentication(auth)
      .processor(new StringDelimitedProcessor(queue)).build()
    client.connect()

    // arbitrary limit
    for(i <- 1 until 1000) {
      val record = queue.take()
      // TODO: Do something with this actually
      println(i + ": " + record.toString)

      // convert this into a kinesis put request
      val kinesisrecord = new PutRecordRequest()
      kinesisrecord.setStreamName(streamName)
      kinesisrecord.setData(ByteBuffer.wrap(record.getBytes("UTF-8")))
      kinesisrecord.setPartitionKey("1") // Partition key cannot be null

      kinesisClient.putRecord(kinesisrecord)
    }

    // Cleanup before you go
    client.stop()
    System.exit(0)
  }
}

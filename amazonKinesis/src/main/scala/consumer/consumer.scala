//package consumer
//
//import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
//import com.amazonaws.services.kinesis.AmazonKinesisClient
//import org.apache.spark.streaming.Milliseconds
//import org.apache.spark.streaming.kinesis.KinesisUtils // tmp
//
//object consumer {
//
//  // DEFINE
//  val appName = "TwitterForKinesis"
//  val streamName = "wm2"
//  val endpointUrl = "https://kinesis.us-east-2.amazonaws.com"
//  val initialPosition = "LATEST" // consumer's position in stream
//
//  def main(args: Array[String]): Unit = {
//
//    // AWS Credentials
//    val credentials = new DefaultAWSCredentialsProviderChain().getCredentials()
//    require(credentials != null,
//      "No AWS credentials found. See http://docs.aws.amazon.com/AWSSdkDocsJava/latest/DeveloperGuide/credentials.html")
//
//    // Kinesis Client
//    val kinesisClient = new AmazonKinesisClient(credentials)
//    kinesisClient.setEndpoint(endpointUrl)
//    val numShards = kinesisClient.describeStream(streamName).getStreamDescription().getShards().size
//    val numStreams = numShards
//    val batchInterval = Milliseconds(2000)
//    val kinesisCheckpointInterval = batchInterval
//
//
//    KinesisUtils.
//  }
//}
//
//class RecordProcessor extends IRecordsProcessor

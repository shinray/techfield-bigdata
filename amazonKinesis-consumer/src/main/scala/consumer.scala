package consumer

import com.amazonaws.services.kinesis.clientlibrary.interfaces.{IRecordProcessor, IRecordProcessorCheckpointer, IRecordProcessorFactory}
import com.amazonaws.services.kinesis.model.Record

object consumer {

  // DEFINE
  val appName = "TwitterForKinesis"
  val streamName = "wm2"
  val endpointUrl = "https://kinesis.us-east-2.amazonaws.com"
  val initialPosition = "LATEST" // consumer's position in stream

  def main(args: Array[String]): Unit = {


  }
}

class RecordProcessorFactory()
  extends IRecordProcessorFactory {
  @Override
  def createProcessor: IRecordProcessor = {
    return new RecordProcessor();
  }
}

class RecordProcessor extends IRecordProcessor {

  private var kinesisShardId: String = _

  @Override
  def initialize(shardId: String): Unit = {
    println("Initializing record processor for shard: " + shardId)
    this.kinesisShardId = shardId
  }

  @Override
  def processRecords(records: List[Record],
                     checkpointer: IRecordProcessorCheckpointer): Unit = {


  }

  private def processRecordsWithRetries(records: List[Record]): Unit = {
    println(s"Processing ${records.size} records from $kinesisShardId")
    println("data: " + new String(record.getData.array))
  }
}

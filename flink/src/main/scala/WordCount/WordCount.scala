package WordCount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
//import org.apache.flink.streaming.api.datastream.DataStream
//import org.apache.flink.streaming.api.datastream._
//import org.apache.flink.streaming.api.scala.DataStream

object WordCount {
  def main(args: Array[String]): Unit = {

    val params = ParameterTool.fromArgs(args)
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.getConfig.setGlobalJobParameters(params)

    val text =
      if (params.has("input")) {
        env.readTextFile(params.get("input"))
      } else {
        println("Executing WordCount example with default inputs data set.")
        println("Use --input to specify file input.")
        // get default test text data
        env.fromElements(WordCountData.WORDS: _*)
      }

    val counts : DataStream[(String, Int)] = text
      .flatMap(_.toLowerCase.split("\\W+"))
      .filter(_.nonEmpty)
      .map((_, 1))
      // group by the tuple field "0" and sum up tuple field "1"
      .keyBy(0)
      .sum(1)

    // emit result
    if (params.has("output")) {
      counts.writeAsText(params.get("output"))
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      counts.print()
    }

    // execute program
    env.execute("Streaming WordCount")
  }
}

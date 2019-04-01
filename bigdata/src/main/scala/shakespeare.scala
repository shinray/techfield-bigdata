import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text

import org.apache.hadoop.fs.Path

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import org.apache.hadoop.util.GenericOptionsParser
import scala.collection.JavaConverters._

object Shakespeare {
  // Mapper function. Takes raw input and pairs it with 1
  class shakespeareMapper extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    val word = new Text

    override
    def map(key:Object, value:Text, context:Mapper[Object, Text, Text, IntWritable]#Context) = {
      var input = value.toString().split("\\s+")
      for (i <- input) {
        word.set(i)
        context.write(word, one)
      }
    }
  }

  // Reducer function. Since this is a word count, simply total the words.
  class shakespeareReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override
    def reduce(key:Text, values:java.lang.Iterable[IntWritable], context:Reducer[Text, IntWritable, Text, IntWritable]#Context) = {
      val sum = values.asScala.foldLeft(0) { (t,i) => t + i.get}
      context.write(key, new IntWritable(sum))
    }
  }

  // Main
  def main(args:Array[String]):Unit = {
    val conf = new Configuration()
    val otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs()
    if (otherArgs.length != 2) {
      println("Usage: Shakespeare <in> <out>")
      return 2
    }

    // Create a Hadoop job
    val job = new Job(conf, "Word count")
    job.setJarByClass(Shakespeare.getClass)

    // set the input and output
    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1)))

    // set the Mapper and Reducer classes
    job.setMapperClass(classOf[shakespeareMapper]);
    job.setReducerClass(classOf[shakespeareReducer]);

    // Specify the type of the output
    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    // Run the job
    System.exit(if (job.waitForCompletion(true)) 0 else 1)
  }
}
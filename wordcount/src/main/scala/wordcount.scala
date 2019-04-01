import org.apache.spark._

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setAppName("OneCar").setMaster("local[*]")

    val sc: SparkContext = new SparkContext(conf)

    val lines = sc.textFile("/home/shinray/snap/skype/common/shakespeare.txt")

//    lines.take(1)
    val words = lines.flatMap(line => line.split("\\s+"))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y} // map(_, 1).reducebykey(_+_)
    println(counts.getClass)
//    counts.saveAsTextFile("/home/shinray/scalatest.txt")
    counts.collect().foreach(println)
  }
}

import org.apache.spark.sql.SparkSession

object spark_movie_len {
  def main(args: Array[String]): Unit = {
    val ratingspath = "/home/shinray/Downloads/data/ratings.dat"
    val spark = SparkSession.builder.master("local").appName("spark_movie_len").getOrCreate()

    val data_rdd = sc.textFile(ratingspath)
  }
}
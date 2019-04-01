import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.functions._

object spark_movie_len {
  def main(args: Array[String]): Unit = {
    val ratingspath = "/home/shinray/Downloads/data/ratings.dat"
    val moviespath = "/home/shinray/Downloads/data/movies.dat"
    val userspath = "/home/shinray/Downloads/data/users.dat"
    val spark = SparkSession.builder.master("local").appName("spark_movie_len").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val sqlContext = new SQLContext(spark.sparkContext)
    import sqlContext.implicits._

    // Q1
    val data_rdd = spark.sparkContext.textFile(ratingspath)
    // Q2
    println("data_rdd contains: " + data_rdd.count() + " lines.")
    // Q3
//    val ratingsschema = StructType(List(
//      StructField("userid",IntegerType,true),
//      StructField("movieid",IntegerType,true),
//      StructField("ratings",IntegerType,true),
//      StructField("timestamp",IntegerType,true) // stored as UNIX timestamp
//    ))
    // make stuff into dfs because I'm lazy and I hate thinking
    val ratingsdf = data_rdd.map(_.split("::")).map{case Array(a,b,c,d) =>
      (a.toInt, b.toInt, c.toInt, d.toInt)}.toDF("userid","movieid","ratings","timestamp")
    val onestars = ratingsdf.select(ratingsdf.col("*"))
      .filter("ratings = 1")
    println("detected 1-star ratings: " + onestars.count())
    // Q4
    val uniquemovies = ratingsdf.select("movieid").distinct().count()
    println("counted " + uniquemovies + " distinct movies")
    // Q5
    val mostactiveuser = ratingsdf.groupBy("userid").count().sort(desc("count")).limit(1)
    println("user with most ratings: ")
    mostactiveuser.take(1).foreach(println) // I'm sure there's a cleaner way to do this, but...
    // Q6
    val happiestuser = ratingsdf.filter("ratings = 5").groupBy("userid").count().sort(desc("count")).limit(1)
    println("user with most 5 star ratings: ")
    happiestuser.take(1).foreach(println)
    // Q7
    val popularmovie = ratingsdf.groupBy("movieid").count().sort(desc("count")).limit(1)
    println("movie with the most ratings: ")
    popularmovie.take(1).foreach(println)
    // Q8
    val movies_rdd = spark.sparkContext.textFile(moviespath)
    println("movies_ contains: " + movies_rdd.count() + " lines.")
    val users_rdd = spark.sparkContext.textFile(userspath)
    println("users_rdd contains: " + users_rdd.count() + " lines.")
    // Q9
//    val tostringarray = udf((value: String) => value.split("\\|").map(_.toString))
    // The genres column is pipe-delimited. need to split again somehow...
    val moviesdf = movies_rdd.map(_.split("::")).map{case Array(a,b,c) =>
      (a.toInt, b, c)}.toDF("movieid","title","genres")
      //.withColumn("genres", split(moviesdf("genres"), "\\|"))
//      .withColumn("genre", tostringarray(moviesdf("genres"))).show
//    moviesdf.createOrReplaceTempView("movies")
//    spark.sqlContext.sql("SELECT * FROM movies WHERE genres LIKE '%Comedy%'")
    val usersdf = users_rdd.map(_.split("::")).map{case Array(a,b,c,d,e) =>
      (a.toInt, b, c.toInt, d.toInt, e.toInt)}.toDF("userid","gender","age","occupation","zipcode")
    // use triple quotes for pipe since double will interpret as a regex, and | is a control char
    val comedies = moviesdf.filter(moviesdf.col("genres").like("%Comedy%"))
    println("found " + comedies.count() + " comedies")
      //moviesdf.withColumn("genres", split(moviesdf.col("genres"), "\\|"))
//    moviesdf.printSchema()
    //val tmp = moviesdf.select("genres").where(array_contains(moviesdf("genres"),"Comedy")).show()
    // Q10
    val mostratedcomedies = comedies.join(ratingsdf, comedies.col("movieid") === ratingsdf.col("movieid"))
  .groupBy(ratingsdf.col("movieid")).count().sort(desc("count")).limit(1)
//    val ratingscount = mostratedcomedies.c
//    mostratedcomedies.show()
//    println(mostratedcomedies.count())
    println("the comedy with the most ratings: ")
    mostratedcomedies.take(1).foreach(println)
//    val mostratedcomedies = ratingsdf.select().filter()
//    mostratedcomedies.select(comedies.col("title"),)
  }
}
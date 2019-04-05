import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.cassandra._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object cassandra_movielens {

  // Spark stuff
  val spark = SparkSession.builder
    .config("spark.cassandra.connection.host","localhost")
    .config("spark.cassandra.connection.port","9042")
    .master("local[*]").appName("cassandraMovielens")
    .getOrCreate()

  // Cassandra stuff
  val connector = CassandraConnector.apply(spark.sparkContext.getConf)

  // u.user schema
  val schema = StructType(List(
    StructField("user_id",IntegerType,false),
    StructField("age",IntegerType,true),
    StructField("gender",StringType,true),
    StructField("occupation",StringType,true),
    StructField("zip",StringType,true)
  ))

  def main(args: Array[String]): Unit = {

    val inpath = "/home/shinray/Downloads/HadoopMaterials/ml-100k/u.user"
    // Grab data from input
    val infile = spark.sparkContext.textFile(inpath)
    // Split on pipe delimited. u.user has 5 columns
    val row = infile.map(_.split("\\|")).map(t => Row(t(0).toInt,t(1).toInt,t(2),t(3),t(4)))
    // Put it all together and what do you get?
    val df = spark.sqlContext.createDataFrame(row,schema)
//    df.show() // SUCCess it works

    // Connect to Cassandra
//    val session = connector.openSession()
    // Give Cassandra the dataframe
    df.write
      .format("org.apache.spark.sql.cassandra")
      .mode("append")
      .options(Map(
        "table" -> "users",
        "keyspace" -> "movielens"
      ))
      .save()

    // Take the dataframe from Cassandra
    val cassandraD = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map(
        "table" -> "users",
        "keyspace" -> "movielens"
      ))
      .load()
//    cassandraD.show()

    // SQL version of the Dataframe
    cassandraD.createOrReplaceTempView("users")

    // Gimme gimme gimme gimme that sql
    val sqlD = spark.sql("SELECT * FROM users WHERE age < 20")
    // Show me what you got
    sqlD.show()

    // Goodbye ❤
    println("Goodbye ❤")
    spark.stop()
  }
}

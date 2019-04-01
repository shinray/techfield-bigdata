import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, IntegerType, StringType, ArrayType, BooleanType, DateType, FloatType}

object batch {
  val eventpath = "hdfs://sandbox.hortonworks.com:8020/user/maria_dev/med_input/events.csv"
  val mortpath = "hdfs://sandbox.hortonworks.com:8020/user/maria_dev/med_input/mortality.csv"
  val outpatha = "hdfs://sandbox.hortonworks.com:8020/user/maria_dev/sparkbatch/alive"
  val outpathd = "hdfs://sandbox.hortonworks.com:8020/user/maria_dev/sparkbatch/dead"
//  val eventpath = "/home/shinray/Downloads/techfield_bigdata/data/mortality/events.csv"
//  val mortpath = "/home/shinray/Downloads/techfield_bigdata/data/mortality/mortality.csv"
//  val outpatha = "/home/shinray/sparkbatch-output/alive"
//  val outpathd = "/home/shinray/sparkbatch-output/dead"
  val spark = SparkSession.builder.master("local").appName("onecar").getOrCreate()
  //.config("spark.sql.warehouse.dir", warehouseLocation)


  def main(args: Array[String]): Unit = {

//    val eventSchema = StructType(List(
//      StructField("patientId",IntegerType,false),
//      StructField("eventId",StringType,true),
//      StructField("eventDescription",StringType,true),
//      StructField("timestamp",DateType,true),
//      StructField("value",FloatType, true)
//    ))
////    val timestampFormat = "yyyy-MM-dd"
//    val mortalitySchema = StructType(List(
//      StructField("patientId",IntegerType,false),
//      StructField("timestamp",DateType,false),
//      StructField("label",BooleanType,false)
//    ))
    var mortdf = spark.read.csv(mortpath)
    mortdf = (
      mortdf
//        .withColumnRenamed("_c0","patientid")
        .withColumn("patientid", mortdf.col("_c0").cast(IntegerType))
        .drop("_c0")
        .withColumn("timestamp", mortdf.col("_c1").cast(DateType))
        .drop("_c1")
//        .withColumnRenamed("_c1","timestamp")
        .withColumnRenamed("_c2", "label")
      )

    var eventdf = spark.read.csv(eventpath)
    eventdf = (eventdf
        .withColumn("patientid", eventdf.col("_c0").cast(IntegerType))
        .drop("_c0")
        .withColumnRenamed("_c1","eventid")
        .withColumnRenamed("_c2","eventdescription")
        .withColumn("timestamp",eventdf.col("_c3").cast(DateType))
        .drop("_c3")
        .withColumn("value",eventdf.col("_c4").cast(FloatType))
        .drop("_c4")
    )

    eventdf.createOrReplaceTempView("events")
    eventdf.cache()
    mortdf.createOrReplaceTempView("mortality")
    mortdf.cache()

    val alivedf = spark.sql(
      "SELECT * FROM events WHERE NOT EXISTS (SELECT patientid FROM mortality WHERE mortality.patientid = events.patientid)"
    )
    alivedf.show()
    alivedf.createOrReplaceTempView("alive")
    alivedf.cache()
//
//    val aresultdf = spark.sql(
//      "SELECT "
//    )
    var alivedf2 = alivedf.groupBy("patientid").count().select(avg("count"),min("count"),max("count"))
    alivedf2.show()
    alivedf2.write.option("header",true).csv(outpatha)

    val deaddf = spark.sql(
      "SELECT * FROM events WHERE EXISTS (SELECT patientid FROM mortality WHERE mortality.patientid = events.patientid)"
    )
    deaddf.show()
    deaddf.createOrReplaceTempView("dead")
    deaddf.cache()

    var deaddf2 = deaddf.groupBy("patientid").count().select(avg("count"),min("count"),max("count"))
    deaddf2.show()
    deaddf2.write.option("header",true).csv(outpathd)
  }
}

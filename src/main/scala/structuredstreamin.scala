import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.StructType._
import org.apache.spark.sql._

/* Created by shishir on 29/06/17.
  */
object structuredstreamin {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("structuredstreamin")
      .getOrCreate()

    import spark.implicits._

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.144.102.121:9092,10.144.102.122:9092,10.144.102.123:9092,10.144.102.124:9092")
      .option("auto.reset.offset", "smallest")
      .option("subscribe","gg-sample")
      .option("enable.auto.commit", "false")
      .option("startingOffsets", "earliest")
      .option("checkpointLocation", "hdfs://JMNGD1BAX310V08:8020/checkpoint_ds1/")
      .option(" failOnDataLoss", "false")
      .load()

    val before = StructType(
      StructField("ID", StringType, false) ::
        StructField("FIRSTNAME", StringType, false) ::
        StructField("BIRTHDAY", StringType, false) :: Nil)

    val schema =
      StructType(
        StructField("table", StringType, false) ::
          StructField("op_type", StringType, false) ::
          StructField("op_ts", StringType, false) ::
          StructField("current_ts", BooleanType, false) ::
          StructField("pos", StringType, false) ::
          StructField("before", before, false) ::
          StructField("after", before, false)
          :: Nil)

    /*
    import org.apache.spark.sql.functions.from_json
    import org.apache.spark.sql.types._
    val tmp = ds1.withColumn("jsonData", from_json($"value".cast("string"), schema))

    val tmp1 = tmp.select("jsonData.after").printSchema

     */
    val table_name = ds1.withColumn("table", get_json_object(($"value").cast("string"), "$.table"))

    val kafka1 = table_name.filter($"table" === "shishir").select("key","value")

    val kafka2 = table_name.filter($"table" === "shishi").select("key", "value")

    val df1 = kafka1
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.144.102.121:9092,10.144.102.122:9092,10.144.102.123:9092,10.144.102.124:9092")
      .option("topic", "shishir")
      .option("checkpointLocation", "hdfs://JMNGD1BAX310V08:8020/checkpoints/shishir")
      .start()

    val df2 = kafka2
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "10.144.102.121:9092,10.144.102.122:9092,10.144.102.123:9092,10.144.102.124:9092")
      .option("topic", "shishi")
      .option("checkpointLocation", "hdfs://JMNGD1BAX310V08:8020/checkpoints/shishi")
      .start()

    df1.awaitTermination()
    df2.awaitTermination()

  }

}

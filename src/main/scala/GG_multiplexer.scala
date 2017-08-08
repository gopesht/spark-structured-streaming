import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.types._

/* Created by shishir on 29/06/17.
  */
object GG_multiplexer {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GG_multiplexer")
      .getOrCreate()

    import spark.implicits._

    val ctxdb = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "x.x.x.x1:9092,x.x.x.x2:9092,x.x.x.x3:9092,x.x.x.x4:9092")
      .option("subscribe","kafka-ctxdb")
      .option("startingOffsets", "earliest")
      .option(" failOnDataLoss", "false")
        .option("checkpointLocation","hdfs://namenode:8020/checkpoints/kafka-ctxdb-input/")
      .load()

    /*
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
          StructField("before", StringType, false) ::
          StructField("after", StringType, false)
          :: Nil)
    */

    val table_name = ctxdb.withColumn("table", get_json_object(($"value").cast("string"), "$.table"))

    val kafka1 = table_name.filter($"table" === "table1").select("key","value")
    val kafka2 = table_name.filter($"table" === "table2").select("key", "value")

    val topic1 = "topic.out.1"
    val checkpoint1 = "hdfs://namenode:8020/checkpoints/1"
    val topic2 = "topic.out.2"
    val checkpoint2 = "hdfs://namenode:8020/checkpoints/2"




    val df1 = kafka1
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "x.x.x.x1:9092,x.x.x.x2:9092,x.x.x.x3:9092,x.x.x.x4:9092")
      .option("topic", topic1)
      .option("checkpointLocation", checkpoint1)
      .trigger(ProcessingTime("1 seconds"))
      .start()

    val df2 = kafka2
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "x.x.x.x1:9092,x.x.x.x2:9092,x.x.x.x3:9092,x.x.x.x4:9092")
      .option("topic", topic2)
      .option("checkpointLocation", checkpoint2)
      .trigger(ProcessingTime("1 seconds"))
      .start()

    df1.awaitTermination()
    df2.awaitTermination()

  }

}

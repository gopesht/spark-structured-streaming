import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object tableFInder {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GG_PREPROCESSOR")
      .getOrCreate()

    import spark.implicits._

    val ds1 = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "x.x.x.x:9092,x.x.x.x:9092,x.x.x.x:9092,x.x.x.x:9092")
      .option("subscribe", "kafka-topic")
      .option("startingOffsets", "earliest")
      .option("checkpointLocation", "hdfs://namenode:8020/tmp/ctxdb/")
      .option(" failOnDataLoss", "false")
      .load()


    val table_name = ds1.withColumn("table", get_json_object(($"value").cast("string"), "$.table"))

    import org.apache.spark.sql.streaming.ProcessingTime

    val query = table_name.select("table").distinct()
    val t = query.writeStream.outputMode("update")
      .format("console")
      .trigger(ProcessingTime("5 seconds"))
      .start()

    t.awaitTermination()
  }
}

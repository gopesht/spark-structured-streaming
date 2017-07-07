import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.sql.{DataFrame, SparkSession}
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
      .option("subscribe","stream-1")
      .option("startingOffsets", "latest")
      .load()


    val res = ds1.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)].toDF

    val query = res.select("*")
    import org.apache.spark.sql.streaming.ProcessingTime
    val t= query.writeStream.outputMode("update")
      .format("console")
      .trigger(ProcessingTime("5 seconds"))
      .start()

    t.awaitTermination()
  }

}

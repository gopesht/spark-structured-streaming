import org.apache.spark
import org.apache.spark.implicits._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType};
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.year


import org.apache.spark.sql.functions.explode


import org.apache.spark.sql._
object json_expander {
  def children(colname: String, df: DataFrame) = {
    val parent = df.schema.fields.filter(_.name == colname).head
    val fields = parent.dataType match {
      case x: StructType => x.fields
      case _ => Array.empty[StructField]
    }
    val tmp = fields.map(x => col(s"$colname.${x.name}"))
    tmp :+ col("op_ts")

  }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("GG_PREPROCESSOR")
      .getOrCreate()

    import spark.implicits._

    //val sqlDF = sqlContext.read.json("hdfs://namenode:8020/GG_sample.json")
    val sqlDF = spark.read.json("hdfs:///data/2017/08/08/10")
    sqlDF.printSchema
    sqlDF.show

    val tmp = sqlDF
      .withColumn("op_ts", $"op_ts".cast("timestamp"))  // cast to timestamp
    //  .withColumn("year", year($"op_ts"))
    //  .withColumn("month", month($"op_ts"))
    //  .withColumn("day", dayofmonth($"op_ts"))
    //  .withColumn("unix_ts", unix_timestamp($"op_ts","YYYY-MM-DD HH:MM:ss.SSSSSS"))
    //  .drop("current_ts")
    //  .drop("pos")

    tmp.select(children("after", tmp): _*)
      .withColumn("unix_ts", unix_timestamp($"EVDATETIME","YYYY-MM-dd:HH:mm:ss.SSSSSSSSS"))
      .withColumn("op_ts1", $"unix_ts".cast("timestamp"))  // cast to timestamp
      .withColumn("year", year($"op_ts1"))
      .withColumn("month", month($"op_ts1"))
      .withColumn("day", dayofmonth($"op_ts1"))
      .show
  }

}

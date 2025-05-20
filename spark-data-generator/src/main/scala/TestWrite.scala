import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{to_utc_timestamp, from_unixtime, monotonically_increasing_id, to_date}


object TestWrite {
  def run(storagePath: String, spark: SparkSession): Unit = {
    def randomStringGen(length: Int) = scala.util.Random.alphanumeric.take(length).mkString
    val columns = 5

    val rdd = spark.sparkContext.parallelize(1 to 1000, 1).map(x => {
      var values = Seq[String]()
      for (i <- 1 to columns)
        values = values :+ randomStringGen(6)
      Row.fromSeq(values)
    })

    var fields = Seq[StructField]()
    for (i <- 1 to columns)
      fields = fields :+ StructField("col_"+i.toString, StringType, true)

    val df = spark.createDataFrame(rdd, StructType(fields))
      .withColumn("id", monotonically_increasing_id())

    df.write.mode(SaveMode.Overwrite).parquet(storagePath)
  }
}
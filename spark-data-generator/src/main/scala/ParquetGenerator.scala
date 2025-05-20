import org.apache.spark.sql.SparkSession

object ParquetGenerator {
  def main(args: Array[String]): Unit = {

    val mode = args(0)
    val storagePath = args(1)

    // val spark = SparkSession.builder.appName("Data Generator").config("hive.metastore.uris", "http://fs0:9083").enableHiveSupport().getOrCreate()
    val spark = SparkSession.builder.appName("Data Generator").enableHiveSupport().getOrCreate()

    if (mode == "test") {
      TestWrite.run(storagePath, spark)
    } else if (mode == "datagen") {
      val dsdgenPath = args(2)
      val scaleFactor = args(3)
      Datagen.data(storagePath, dsdgenPath, spark, scaleFactor)
    } else if (mode == "metagen") {
      Datagen.metadata(storagePath, spark)
    } else if (mode == "query") {
      val query = args(2)
      Query.run(query, spark)
    } else {
      throw new IllegalArgumentException("Unknown mode: " + mode)
    }
  }
}

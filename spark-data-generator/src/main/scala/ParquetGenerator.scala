import org.apache.spark.sql.SparkSession

object ParquetGenerator {
  def main(args: Array[String]): Unit = {
    println("🧪 Debug: args length = " + args.length)
    args.zipWithIndex.foreach { case (arg, i) => println(s"🧪 args($i): $arg") }
    val mode = args(0)
    val storagePath = args(1)
    println("🟠 Mode is: " + mode)
    val spark = SparkSession.builder.appName("Data Generator").enableHiveSupport().getOrCreate()

    mode match {
      case "test" =>
        TestWrite.run(storagePath, spark)

      case "datagen" =>
        val dsdgenPath = args(2)
        val scaleFactor = args(3)
        Datagen.data(storagePath, dsdgenPath, spark, scaleFactor)

      case "metagen" =>
        println("🔵 进入 metagen 分支")
        val storagePath = args(1)
        val scaleFactor = args(2)
        Datagen.metadata(storagePath, scaleFactor, spark)

      case "query" =>
        val query = args(1)
        val scaleFactor = args(2)
        Query.run(query, spark, scaleFactor)

      case _ =>
        throw new IllegalArgumentException("Unknown mode: " + mode)
    }
  }
}

import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

object Datagen {
  def data(storagePath: String, dsdgenPath: String, spark: SparkSession, scaleFactor: String): Unit = {
    val sqlContext = spark.sqlContext

    val databaseName = s"dataset_tpcds_${scaleFactor}g" // name of database to create.
    val format = "parquet" // valid spark format like parquet "parquet".
    // Run:
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = dsdgenPath, // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType
    val location = s"${storagePath}/${databaseName}"

    tables.genData(
      location = location,
      format = format,
      overwrite = true, // overwrite the data that is already there
      partitionTables = true, // create the partitioned fact tables
      clusterByPartitionColumns = true, // shuffle to get partitions coalesced into single files.
      filterOutNullPartitionValues = false, // true to filter out the partition with NULL key value
      tableFilter = "", // "" means generate all tables
      numPartitions = scaleFactor.toInt) // how many dsdgen partitions to run - number of input tasks.
  }

  def metadata(storagePath: String, scaleFactor: String, spark: SparkSession): Unit = {
    val sqlContext = spark.sqlContext

    val databaseName = s"dataset_tpcds_${scaleFactor}g"
    val format = "parquet"
    

    val tables = new TPCDSTables(sqlContext, dsdgenDir = "", scaleFactor = scaleFactor,
      useDoubleForDecimal = false, useStringForDate = false)
    val location = s"$storagePath/$databaseName"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName LOCATION '$location'")
    tables.createExternalTables(location, format, databaseName, overwrite = true, discoverPartitions = true)
    tables.analyzeTables(databaseName, analyzeColumns = true)
    println("========== DEBUG ==========")
    println("🟡 当前数据库列表：")
    spark.catalog.listDatabases().show(false)

    println("🟢 查看 dataset_tpcds_1g 中的表：")
    spark.catalog.listTables(databaseName).show(false)

    println("🔵 当前 session 使用的 database 是：" + spark.catalog.currentDatabase)

  }
}

import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDSTables

object Datagen {
  def data(storagePath: String, dsdgenPath: String, spark: SparkSession, scaleFactor: String): Unit = {
    val sqlContext = spark.sqlContext

    val databaseName = s"dataset_tpcds_${scaleFactor}G" // name of database to create.
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

  def metadata(storagePath: String, spark: SparkSession): Unit = {
    val sqlContext = spark.sqlContext

    val databaseName = s"dataset_tpcds_100G" // name of database to create.
    val scaleFactor = "100" // scaleFactor defines the size of the dataset to generate (in GB).
    val format = "parquet" // valid spark format like parquet "parquet".
    // Run:
    val tables = new TPCDSTables(sqlContext,
      dsdgenDir = "", // location of dsdgen
      scaleFactor = scaleFactor,
      useDoubleForDecimal = false, // true to replace DecimalType with DoubleType
      useStringForDate = false) // true to replace DateType with StringType
    val location = s"${storagePath}/${databaseName}"

    spark.sql(s"create database if not exists $databaseName location '${location}'")
    // Create metastore tables in a specified database for your data.
    // Once tables are created, the current database will be switched to the specified database.
    tables.createExternalTables(location, format, databaseName, overwrite = true, discoverPartitions = true)
    // Or, if you want to create temporary tables
    // tables.createTemporaryTables(location, format)

    // For CBO only, gather statistics on all columns:
    tables.analyzeTables(databaseName, analyzeColumns = true)
  }
}

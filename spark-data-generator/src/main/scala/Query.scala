import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDS


object Query {
  def run(queryName: String, spark: SparkSession, scaleFactor: String, repeat: Int = 1): Unit = {

    val sqlContext = spark.sqlContext
    val storagePath = "/tpcds-data"
    val databaseName = s"dataset_tpcds_${scaleFactor}g"
    val location = s"$storagePath/$databaseName"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName LOCATION '$location'")
    spark.sql(s"USE $databaseName")

    val tpcds = new TPCDS(sqlContext = sqlContext)
    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)

    for (i <- 1 to repeat) {
      println(s"\n🔁 Running $queryName - iteration $i/$repeat")
      tpcds.run(queryToRun)
    }
  }
}

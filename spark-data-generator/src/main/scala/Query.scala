import org.apache.spark.sql.SparkSession
import com.databricks.spark.sql.perf.tpcds.TPCDS

object Query {
  def run(queryName: String, spark: SparkSession, scaleFactor: String): Unit = {
    
    val sqlContext = spark.sqlContext
    val storagePath = "/tpcds-data"
    val databaseName = s"dataset_tpcds_${scaleFactor}g"
    val location = s"$storagePath/$databaseName"
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName LOCATION '$location'")
    spark.sql(s"USE $databaseName")

    val tpcds = new TPCDS(sqlContext = sqlContext)
    val queryToRun = tpcds.tpcds2_4Queries.filter(q => q.name == queryName)
    val allQueries = tpcds.tpcds2_4Queries
    println(s"📋 当前可用的 query 名称有：")
    allQueries.map(_.name).foreach(println)
    tpcds.run(queryToRun)
  }
}

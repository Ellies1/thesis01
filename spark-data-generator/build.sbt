name := "Parquet Data Generator"

version := "1.0"

scalaVersion := "2.12.18"

val currentDirectory = new java.io.File(".").getCanonicalPath

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.4.1"
libraryDependencies += "com.databricks" % "spark-sql-perf_2.12" % "0.5.1" from ("file://" + currentDirectory + "/docker/picklejars/spark-sql-perf_2.12-0.5.1-SNAPSHOT.jar")

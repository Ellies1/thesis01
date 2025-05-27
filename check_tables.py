# check_tables.py
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CheckHiveTables") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=/tpcds-data/metastore_db;create=true") \
    .config("spark.sql.warehouse.dir", "/tpcds-data/hive-warehouse") \
    .getOrCreate()

spark.sql("SHOW TABLES").show(truncate=False)

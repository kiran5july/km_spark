

lstColumns=spark.read.parquet("/km/data/").columns
parquetColumns=spark.createDataFrame(lstColumns, StringType()).withColumnRenamed("value","col_name")
parquetColumns.count()

tblColumns=spark.sql("DESCRIBE kmdb.orders").select("col_name").filter(F.col("col_name").startswith("#")==False)
tblColumns.count()

#---extra in Parquet
parquetColumns.subtract(tblColumns).show(200,False)

#---extra in Hive table
tblColumns.subtract(parquetColumns).show(200,False)

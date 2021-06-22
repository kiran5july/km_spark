
#---columns from parquet data
lstColumns=spark.read.parquet("/km/data/").columns
df_parquetColumns=spark.createDataFrame(lstColumns, StringType()).withColumnRenamed("value","col_name")
df_parquetColumns.count()


#---columns from table
df_tblColumns=spark.sql("DESCRIBE kmdb.orders").select("col_name").filter(F.col("col_name").startswith("#")==False)
df_tblColumns.count()

#---extra in Parquet
df_parquetColumns.subtract(df_tblColumns).show(200,False)

#---extra in Hive table
df_tblColumns.subtract(df_parquetColumns).show(200,False)



#----- look up or matching for a string in columns

[i for i in spark.read.parquet('/km/data/').columns if 'tran' in i]

[i for i in lst if lstColumns.find('order')>=0]





df = spark.createDataFrame([('a','h4k2p'), ('b','12345')]).toDF('id','postal_code')

#return only if postal_code contains numeric data
df.filter((F.col('postal_code').cast('int').isNotNull()) ).show(5)


.filter(F.col("extract_date").between("2020-04-09","2020-04-09"))


.select( F.when( F.col("v1").isNull() | F.col("v2").isNull(), 1).otherwise(1).alias("sha_counts") )



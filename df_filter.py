


df = spark.createDataFrame([('a','h4k2p'), ('b','12345')]).toDF('id','postal_code')


#return only if postal_code contains numeric data
df.filter((F.col('postal_code').cast('int').isNotNull()) ).show(5)


df.filter(F.col("extract_date").between("2020-04-09","2020-04-09"))

#multiple conditions (and/or )
df.filter( (F.col('col1')=='ABC') & (F.col('col2')=='DEF') )
df.filter( (F.col('col1')=='ABC') | (F.col('col2')=='DEF') )

df.filter( (F.col('id').isNull()) | (F.col('id')=='') )
df.filter( (F.col('id').isNotNull()) & (F.length(F.trim(F.col('id')))>=1) )


#filter if a column contains specific characters
df.filter(df['col1'].rlike('[\!\@\$\^\&\-\_\;\:\?\.\#\*]'))


#when..otherwise (same as case)
df.select( F.when( F.col("v1").isNull() | F.col("v2").isNull(), 1).otherwise(1).alias("sha_counts") )

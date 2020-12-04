


df_id_len = spark.createDataFrame([("abcd", 3), ("efgh", 1), ("ijklm", 2), ("nopqr", 5)], ["id","cnt"])

df_id_len \
 .select('id','cnt',
         O.length(O.col("id")).alias("id_length1"), 
         O.expr("substring(id, length(id)-1, 2)").alias("last2"),
         O.expr('substring(id, 0,cnt)').alias('sub_from_column') 
 ) \
 .show()

 
+-----+---+----------+-----+---------------+
|   id|cnt|id_length1|last2|sub_from_column|
+-----+---+----------+-----+---------------+
| abcd|  3|         4|   cd|            abc|
| efgh|  1|         4|   gh|              e|
|ijklm|  2|         5|   lm|             ij|
|nopqr|  5|         5|   qr|          nopqr|
+-----+---+----------+-----+---------------+



from pyspark.sql import functions as F
from pyspark.sql.window import Window


df_data=spark.createDataFrame([("a", 10), ("b", 10), ("b", 7), ("a", 16)], ["id", "rec_count"])

winRecCount=Window.partitionBy("id").orderBy(F.col("rec_count").desc())
df_data.withColumn("rownum", F.row_number().over(winRecCount) ).show()
+---+---------+------+
| id|rec_count|rownum|
+---+---------+------+
|  b|       10|     1|
|  b|        7|     2|
|  a|       16|     1|
|  a|       10|     2|
+---+---------+------+



#Multiple columns

df_data=spark.createDataFrame([("a", "a1", 10,300), ("b","b1", 10,102), ("b", "b1",7,100), ("a", "a2", 10,200)], ["id", "id2", "rec_count","amount"])
winRecCount=Window.partitionBy("id").orderBy(F.col("rec_count").desc(), F.col("amount").desc())
# winRecCount=Window.partitionBy("id", "id2").orderBy(F.col("rec_count").desc(), F.col("amount").desc())

df_data.withColumn("rownum", F.row_number().over(winRecCount) ).show()

+---+---+---------+------+------+
| id|id2|rec_count|amount|rownum|
+---+---+---------+------+------+
|  b| b1|       10|   102|     1|
|  b| b1|        7|   100|     2|
|  a| a1|       10|   300|     1|
|  a| a2|       10|   200|     2|
+---+---+---------+------+------+




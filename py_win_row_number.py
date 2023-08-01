
from pyspark.sql import functions as F
from pyspark.sql.window import Window

df_data=spark.createDataFrame([("a", 10), ("b", 10), ("b", 7), ("a", 16), ("a", 20)], ["id", "rec_count"])

#---simple row_number()
#NOTE: ordering is MUST; if not, gives pyspark.sql.utils.AnalysisException: u'Window function row_number() requires window to be ordered, please add ORDER BY clause. For example SELECT row_number()(value_expr) OVER (PARTITION BY window_partition ORDER BY window_ordering) from table;'

w_rn = Window.orderBy(O.col("id") )

df_data.withColumn("rownum", O.row_number().over(w_rn)).show(5,False)
+---+---------+------+
|id |rec_count|rownum|
+---+---------+------+
|a  |16       |1     |
|a  |20       |2     |
|a  |10       |3     |
|b  |7        |4     |
|b  |10       |5     |
+---+---------+------+


#---add partitionby column & order by record count
from pyspark.sql.window import Window
winRecCount=Window.partitionBy("id").orderBy(F.col("rec_count").desc())
df_data.withColumn("rownum", F.row_number().over(winRecCount) ).show()
+---+---------+------+
| id|rec_count|rownum|
+---+---------+------+
|  b|       10|     1|
|  b|        7|     2|
|  a|       20|     1|
|  a|       16|     2|
|  a|       10|     3|
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

#-----ordering for data, blank & None/NULL (DESC: data, blank, NULL)
winId = Window.partitionBy("id").orderBy(F.col('name').desc())
df1 = spark.createDataFrame([("1", "abc"), ("1", ""), ("1", None)]).toDF("id", "name")
df1.withColumn('rn', F.row_number().over(winId) ).show(5)
+---+----+---+
| id|name| rn|
+---+----+---+
|  1| abc|  1|
|  1|    |  2|
|  1|null|  3|
+---+----+---+

#--------------------aggregate functions ----------------

#---agg() using groupBy()
df_data.groupBy("id").agg( 
   F.max("rec_count").alias("max_rec_count")
 , F.count("rec_count").alias("cnt_rec_count")
 , F.sum("rec_count").alias("sum_rec_count") ).show()


#---agg using Window()
winId=Window.partitionBy("id").orderBy("rec_count")
winRowNum=Window.partitionBy("id").orderBy(F.col("row_num").desc())
df_data.select("id", "rec_count"
 , F.row_number().over(winId).alias("row_num")
 , F.max("rec_count").over(winId).alias("max_rec")
 , F.count("id").over(winId).alias("count_rec")
 , F.sum("rec_count").over(winId).alias("sum_rec")
).select("id", F.max("max_rec").over(winRowNum).alias("max_rec")
 , F.max("count_rec").over(winRowNum).alias("cnt_rec_count")
 , F.max("sum_rec").over(winRowNum).alias("sum_rec_count") ).filter(F.col("row_num")==1).show()





----agg() with windowing on Multiple columns
df_id_zip=spark.createDataFrame([("a", "a.1", 2, 10), ("a", "a.1", 1, 12), ("a", "a.2", 1, 6), ("a", "a.2", 3, 12), ("b", "b.1", 7, 11)]
  , ["id", "zip", "rec_count", "total"])

#---agg using groupBy()
df_id_zip.groupBy("id","zip").agg( F.sum("rec_count").alias("sum_rec_count"), F.sum("total").alias("sum_total") ).show()
+---+---+-------------+---------+
| id|zip|sum_rec_count|sum_total|
+---+---+-------------+---------+
|  b|b.1|            7|       11|
|  a|a.1|            3|       22|
|  a|a.2|            4|       18|
+---+---+-------------+---------+

#---agg using Window()
#used as replacement for group
winIdZip=Window.partitionBy("id","zip")

#I need single max record for each "id", so only one field in this window
winMax=Window.partitionBy("id").orderBy(F.col("sum_count").desc(), F.col("sum_total").desc())

df_id_zip.select("id","zip", "rec_count","total"
 , F.sum("rec_count").over(winIdZip).alias("sum_count")
 , F.sum("total").over(winIdZip).alias("sum_total")
).select("id","zip"
 , F.row_number().over(winMax).alias("row_num")
 , F.max("sum_count").over(winMax).alias("sum_rec_count")
 , F.max("sum_total").over(winMax).alias("sum_total") ).show()

+---+---+-------------+---------+
| id|zip|sum_rec_count|sum_total|
+---+---+-------------+---------+
|  b|b.1|            7|       11|
|  a|a.1|            4|       22|
|  a|a.2|            4|       18|
+---+---+-------------+---------+






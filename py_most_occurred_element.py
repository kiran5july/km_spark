


df = spark.createDataFrame([("a", 10), ("a", 10), ("a", 11), ("b", 7), ("b", 8)]).toDF("id", "rec_count")
df.orderBy('id').show(10)

+---+---------+
| id|rec_count|
+---+---------+
|  a|       10|
|  a|       10|
|  a|       11|
|  b|        8|
|  b|        7|
+---+---------+


Output:
+---+---------+
| id|rec_count|
+---+---------+
|  a|       10|
|  b|        8| -> or 7 as both are single occurrances
+---+---------+
import pyspark.sql.functions as F


---#1: using python list function

from collections import Counter

@F.udf
def most_occurred(x):
    try:
        result = Counter(x).most_common(1)[0][0]
    except:
        result = ""
    return result

#see list
#df.groupBy('id').agg(F.collect_list('rec_count').alias('cnts_list')).show(5)

df.groupBy('id').agg(most_occurred(F.collect_list('rec_count')).alias('cnts')).show(5)
+---+----+
| id|cnts|
+---+----+
|  b|   8|
|  a|  10|
+---+----+


---#2: using Spark Window
from pyspark.sql import Window

w_id = Window.partitionBy('id','rec_count')
df_ttls = df.withColumn('rec_count_totals', F.count('*').over(w_id))

df_ttls.show(10)

w_rec_cnt_ttl = Window.partitionBy('id').orderBy(F.col('rec_count_totals').desc())
df_ttls.withColumn('rec_cnt_order', F.row_number().over(w_rec_cnt_ttl)) \
 .filter(F.col('rec_cnt_order')==1).show(10)

+---+---------+----------------+-------------+
| id|rec_count|rec_count_totals|rec_cnt_order|
+---+---------+----------------+-------------+
|  b|        8|               1|            1|
|  a|       10|               2|            1|
+---+---------+----------------+-------------+

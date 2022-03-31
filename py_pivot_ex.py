


df_id_yr_counts = spark.createDataFrame([("1", "2020", 16), ("1", "2021", 7), ("2", "2019", 24), ("2", "2021", 15)]) \
 .toDF("id", "year", "no_of_orders")

+---+----+------------+
| id|year|no_of_orders|
+---+----+------------+
|  1|2020|          16|
|  1|2021|           7|
|  2|2019|          24|
|  2|2021|          15|
+---+----+------------+

import pyspark.sql.functions as F
df_id_yr_counts.groupBy('id').pivot('year').agg(F.sum('no_of_orders').alias('no_of_orders')).orderBy('id').show(5)
+---+----+----+----+
| id|2019|2020|2021|
+---+----+----+----+
|  1|null|  16|   7|
|  2|  24|null|  15|
+---+----+----+----+



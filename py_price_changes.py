#------show the percentage of price change from previous time
#sample data
part_id|unit_price|part_update_timestamp
111|5.99|2022-12-01 07:35:56
111|6.99|2022-12-05 08:12:34
222|25.99|2022-12-02 08:02:45
222|29.89|2022-12-12 08:58:31
333|9.99|2022-12-15 04:38:44

Output:
+-------+----------+---------------------+-------------------+--------------------+
|part_id|unit_price|part_update_timestamp|unit_price_previous|price_change_percent|
+-------+----------+---------------------+-------------------+--------------------+
|    111|      5.99|  2022-12-01 07:35:56|                  0|                null|
|    111|      6.99|  2022-12-05 08:12:34|               5.99|               16.69|
|    222|     25.99|  2022-12-02 08:02:45|                  0|                null|
|    222|     29.89|  2022-12-12 08:58:31|              25.99|               15.01|
|    333|      9.99|  2022-12-15 04:38:44|                  0|                null|
+-------+----------+---------------------+-------------------+--------------------+


df_parts_dim = spark.read.options(sep='|', header=True).csv('/km_practice/data/parts_prices.csv')
import pyspark.sql.functions as F
from pyspark.sql.window import Window
w_old_rec = Window.partitionBy("part_id").orderBy("part_update_timestamp")

df_parts_dim.withColumn('unit_price_previous', F.lag("unit_price",1, 0).over(w_old_rec)) \
 .withColumn('rn', F.row_number().over(w_old_rec)) \
 .withColumn('price_change_percent', F.round(((F.col('unit_price')-F.col('unit_price_previous'))/F.col('unit_price_previous'))*100, 2)) \
 .show(10)

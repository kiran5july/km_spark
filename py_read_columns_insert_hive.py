

--Columns file
#hdfs dfs -put /home/kiran/km/km_big_data/columns_list.csv /km_hadoop/
#Data:
#order_id|order_date|order_customer_id|order_status,orders
#order_item_id|order_item_order_id|order_item_product_id|order_item_quantity|order_item_subtotal|order_item_product_price,order_items

#Read columns file
dfColumns=spark.read.csv("/km_hadoop/columns_list.csv").toDF("columns", "table_name")

#Read data file
dfData=spark.read.load("/km_hadoop/data/ord_data_2020_03_14_14_40_56", format="csv", sep="|", inferSchema="true", header="true")

#Generate columns string by separator
columnsData='|'.join(map(str, dfData.columns))

import pyspark.sql.functions as F
sTable=dfColumns.filter(F.col("columns")==F.lit(columnsData)) \
 .rdd \
 .map(lambda x: x[1]) \
 .collect()[0]


from datetime import date, datetime
extract_date=date.today().strftime('%Y-%m-%d')
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

#Insert in table
dfData.select('order_id', 'order_date', 'order_customer_id', 'order_status', F.lit(extract_date)).write.insertInto("kmdb.orders_prtn")

#Validate
spark.table("kmdb.orders_prtn") \
 .filter(F.col("order_month")=="2019-02-11").count()



from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType,DoubleType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess

print("Input arguments:", sys.argv)
#Check arguments
if (len(sys.argv) != 2):
 print "Incorrect number of input arguments: Expected 2"
 print "1: db name (data_comm/vivid)"
 sys.exit(1)

sDBName = sys.argv[1]

def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("{}:----- Arguments ----------".format(getDT()))
print("1: {}".format(sDBName) )

print("{}: getting spark session..".format(getDT()))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM TAC Merchant Market Share Pass1 Job PySpark") \
 .config("spark.executor.memoryOverhead", "4096") \
 .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#.config("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
#spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#spark.conf.set(spark.yarn.executor.memoryOverhead","4096")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.shuffle.partitions", "25")


print("{}:----- {}.trade_area_compl_mcc_mid_dist_sales -----".format(getDT(), sDBName) )
#(mid, mid_comp, distance, sales, sales_cnt, mcc)
df_mrch_sales = spark.table(sDBName + ".trade_area_compl_mcc_mid_dist_sales")

df_mrch_sales.cache()

df_mrch_mcc = df_mrch_sales.select('mid','mcc').distinct()

df_mrch_sl_not_null = df_mrch_sales.filter(~F.col("sales").isNull())

print("{}:----- Market Share buckets -----".format(getDT()) )

df_ms_5 = df_mrch_sl_not_null.filter(F.col("distance")<=5).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d5'))
df_ms_10 = df_mrch_sl_not_null.filter(F.col("distance")<=10).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d10'))
df_ms_15 = df_mrch_sl_not_null.filter(F.col("distance")<=15).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d15'))
df_ms_20 = df_mrch_sl_not_null.filter(F.col("distance")<=20).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d20'))
df_ms_25 = df_mrch_sl_not_null.filter(F.col("distance")<=25).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d25'))
df_ms_30 = df_mrch_sl_not_null.filter(F.col("distance")<=30).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d30'))
df_ms_35 = df_mrch_sl_not_null.filter(F.col("distance")<=35).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d35'))
df_ms_40 = df_mrch_sl_not_null.filter(F.col("distance")<=40).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d40'))
df_ms_45 = df_mrch_sl_not_null.filter(F.col("distance")<=45).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d45'))
df_ms_50 = df_mrch_sl_not_null.filter(F.col("distance")<=50).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d50'))
df_ms_55 = df_mrch_sl_not_null.filter(F.col("distance")<=55).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d55'))
df_ms_60 = df_mrch_sl_not_null.filter(F.col("distance")<=60).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d60'))
df_ms_65 = df_mrch_sl_not_null.filter(F.col("distance")<=65).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d65'))
df_ms_70 = df_mrch_sl_not_null.filter(F.col("distance")<=70).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d70'))
df_ms_75 = df_mrch_sl_not_null.filter(F.col("distance")<=75).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d75'))
df_ms_80 = df_mrch_sl_not_null.filter(F.col("distance")<=80).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d80'))
df_ms_85 = df_mrch_sl_not_null.filter(F.col("distance")<=85).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d85'))
df_ms_90 = df_mrch_sl_not_null.filter(F.col("distance")<=90).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d90'))
df_ms_95 = df_mrch_sl_not_null.filter(F.col("distance")<=95).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d95'))
df_ms_100 = df_mrch_sl_not_null.filter(F.col("distance")<=100).groupBy('mid','mcc').agg(F.count("*").alias("peer_cnt"), (F.max('sales')/F.sum('sales')).alias('mktshare')).withColumn("dist", F.lit('d100'))


print("{}:     -----> union all".format(getDT()))
df_ms = df_ms_5.unionAll(df_ms_10).unionAll(df_ms_15).unionAll(df_ms_20).unionAll(df_ms_25).unionAll(df_ms_30).unionAll(df_ms_35).unionAll(df_ms_40).unionAll(df_ms_45).unionAll(df_ms_50) \
               .unionAll(df_ms_55).unionAll(df_ms_60).unionAll(df_ms_65).unionAll(df_ms_70).unionAll(df_ms_75).unionAll(df_ms_80).unionAll(df_ms_85).unionAll(df_ms_90).unionAll(df_ms_95).unionAll(df_ms_100)


print("{}:     -----> Build peer count buckets".format(getDT()))
df_ms_peer_cnt = df_ms.groupBy('mid','mcc').pivot('dist').agg(F.first('peer_cnt')) \
 .withColumnRenamed('d5', 'peers_5').withColumnRenamed('d10', 'peers_10').withColumnRenamed('d15', 'peers_15').withColumnRenamed('d20', 'peers_20').withColumnRenamed('d25', 'peers_25') \
 .withColumnRenamed('d30', 'peers_30').withColumnRenamed('d35', 'peers_35').withColumnRenamed('d40', 'peers_40').withColumnRenamed('d45', 'peers_45').withColumnRenamed('d50', 'peers_50') \
 .withColumnRenamed('d55', 'peers_55').withColumnRenamed('d60', 'peers_60').withColumnRenamed('d65', 'peers_65').withColumnRenamed('d70', 'peers_70').withColumnRenamed('d75', 'peers_75') \
 .withColumnRenamed('d80', 'peers_80').withColumnRenamed('d85', 'peers_85').withColumnRenamed('d90', 'peers_90').withColumnRenamed('d95', 'peers_95').withColumnRenamed('d100', 'peers_100')

print("{}:     -----> Build MarketShare buckets".format(getDT()))
df_ms_mktshare = df_ms.groupBy('mid','mcc').pivot('dist').agg(F.first('mktshare')) \
 .withColumnRenamed('d5', 'ms_5').withColumnRenamed('d10', 'ms_10').withColumnRenamed('d15', 'ms_15').withColumnRenamed('d20', 'ms_20').withColumnRenamed('d25', 'ms_25') \
 .withColumnRenamed('d30', 'ms_30').withColumnRenamed('d35', 'ms_35').withColumnRenamed('d40', 'ms_40').withColumnRenamed('d45', 'ms_45').withColumnRenamed('d50', 'ms_50') \
 .withColumnRenamed('d55', 'ms_55').withColumnRenamed('d60', 'ms_60').withColumnRenamed('d65', 'ms_65').withColumnRenamed('d70', 'ms_70').withColumnRenamed('d75', 'ms_75') \
 .withColumnRenamed('d80', 'ms_80').withColumnRenamed('d85', 'ms_85').withColumnRenamed('d90', 'ms_90').withColumnRenamed('d95', 'ms_95').withColumnRenamed('d100', 'ms_100')


print("{}:     -----> Merge all buckets".format(getDT()))
df_ms_final = df_ms_peer_cnt.join(df_ms_mktshare, ['mid','mcc'] ) \
 .select('mid','mcc'
    ,'peers_5','peers_10','peers_15','peers_20','peers_25','peers_30','peers_35','peers_40','peers_45','peers_50'
    ,'peers_55','peers_60','peers_65','peers_70','peers_75','peers_80','peers_85','peers_90','peers_95','peers_100'
    ,'ms_5','ms_10','ms_15','ms_20','ms_25','ms_30','ms_35','ms_40','ms_45','ms_50'
    ,'ms_55','ms_60','ms_65','ms_70','ms_75','ms_80','ms_85','ms_90','ms_95','ms_100'
 )


print("{}:----- Write to {}.truspnd_trade_area_compl_mid_agg -----".format(getDT(), sDBName))
df_ms_final.write.insertInto("{}.truspnd_trade_area_compl_mid_agg".format(sDBName), overwrite=True )

print("{}:----- COMPLETE -----".format(getDT()) )


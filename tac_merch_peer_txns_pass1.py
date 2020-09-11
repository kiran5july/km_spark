
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType,DoubleType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess
from pyspark.sql.window import Window

print("Input arguments:", sys.argv)
#Check arguments
if (len(sys.argv) != 3):
 print "Incorrect number of input arguments: Expected 2"
 print "1: db name (data_comm/vivid)"
 print "2: complaince retain period (numeric)"
 sys.exit(1)

sDBName = sys.argv[1]
iComplRetainPeriod = int(sys.argv[2])
iIncrements = 15000


def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("{}:----- Arguments ----------".format(getDT()))
print("1: {}\n2: {}".format(sDBName,iComplRetainPeriod) )

print("{}: getting spark session..".format(getDT()))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM TAC Merchant Peer Transactions Pass1 Job PySpark") \
 .config("spark.executor.memoryOverhead", "4096") \
 .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

#.config("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#spark.conf.set(spark.yarn.executor.memoryOverhead","4096")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.shuffle.partitions", "25")


print("{}:----- {}.trade_area_compl_mid_compl -----".format(getDT(), sDBName) )
df_mrch_to_process = spark.table(sDBName + ".trade_area_compl_mid_compl") \
 .filter((F.col("last_run_dt").isNull() | (F.col("last_run_dt")<date.today()-timedelta(days=iComplRetainPeriod))) & (F.col("non_compl_reason").isNull() | ~F.col("non_compl_reason").isin(1)) ) \
 .select("merchant_id", "mrch_mcc")


if df_mrch_to_process.count()<=0:
 print("{}:---> No merchants to process.".format(getDT()))
 sys.exit(0)

print("{}:----- {}.trade_area_compl_mcc_mid_lat_lng -----".format(getDT(), sDBName) )
df_mid_lat_lon = spark.table(sDBName + ".trade_area_compl_mcc_mid_lat_lng").repartition("mid").withColumnRenamed("mid", "merchant_id")

print("{}:----- Get Mrch/Lat-Lon -----".format(getDT()))
df_mrch_in_lat = df_mrch_to_process.join(df_mid_lat_lon, ['merchant_id']) \
 .withColumn("rn", F.row_number().over(Window.orderBy('merchant_id')) ) \
 .drop("sic","chain_code")

df_mrch_in_lat.cache()

mids_total = df_mrch_in_lat.count()
print("{}:----- Mids: {}-----".format(getDT(), mids_total) )
if mids_total<=0:
 print("{}:---> No merchants to do compliance check that have Lat/Lon data.".format(getDT()))
 sys.exit(0)

print("{}:----- data_comm.merchant_red -----".format(getDT()) )
df_red = spark.table("data_comm.merchant_red").select(F.trim(F.col("i_chn")).alias("i_chn") )

lst_chns = df_red.rdd.flatMap(lambda x: x).collect()

if len(lst_chns)<=0:
 print("{}:---> Red merchants list is blank, please check.".format(getDT()))
 sys.exit(0)

print("{}:     -----> Red Chains count: {}".format(getDT(), len(lst_chns)) )
b_lst_chns = spark.sparkContext.broadcast(lst_chns)

print("{}:----- Get Non red (peer)-----".format(getDT()) )
df_mid_red_excl_lat_lon = df_mid_lat_lon.filter(~F.col("chain_code").isin(b_lst_chns.value)) \
 .select(F.col("merchant_id").alias("comp_mid"), "sic", F.col("latitude").alias("comp_lat"), F.col("longitude").alias("comp_lon"))

df_mid_red_excl_lat_lon.cache()

print("{}:----- payments.stlmnt_dly -----".format(getDT()) )
df_mrch_tran = spark.table("payments.stlmnt_dly") \
 .filter(~F.col("chain_code").isin(b_lst_chns.value) & F.col("process_date").between( int((date.today()-timedelta(days=377)).strftime('%Y%m%d')) , int((date.today()-timedelta(days=12)).strftime('%Y%m%d')) ) ) \
 .select('merchant_id', F.trim(F.col('mcc')).alias('mcc'), 'sales_amt', 'wic_sales_amt', 'ebt_sales_amt', 'sales_cnt').repartition('merchant_id', 'mcc') \
 .groupBy('merchant_id', 'mcc').agg(F.sum(F.col('sales_amt')+F.col('wic_sales_amt')+F.col('ebt_sales_amt')).alias('sales'), F.sum('sales_cnt').alias('sales_cnt')) \
 .withColumnRenamed('merchant_id', 'comp_mid')

df_mrch_tran.cache()

lowLimit = 0
upperLimit = lowLimit + iIncrements
bOverwrite = True

while lowLimit <= mids_total:
 upperLimit = mids_total if upperLimit>mids_total else upperLimit
 print("{}:----- Range: {} - {}".format(getDT(), lowLimit, upperLimit) )
 
 df_mrch_in_lat_range = df_mrch_in_lat.filter(F.col("rn").between(lowLimit, upperLimit) )

 print("{}:    ----- Join merchants - competitors within 100 miles -----".format(getDT()) )
 df_mid_comp_dist = df_mrch_in_lat_range \
  .join(df_mid_red_excl_lat_lon, (df_mrch_in_lat_range.mrch_mcc == df_mid_red_excl_lat_lon.sic) ) \
  .withColumn("distance", F.round(3958*F.acos(F.sin(F.radians(F.col("latitude")))*F.sin(F.radians(F.col("comp_lat")))+F.cos(F.radians(F.col("latitude")))*F.cos(F.radians(F.col("comp_lat")))*F.cos(F.radians(F.col("comp_lon"))-F.radians(F.col("longitude")))) ,3)  ) \
  .filter((F.col("merchant_id")!=F.col("comp_mid")) & (F.col("distance")<=100)  ) \
  .select('merchant_id', F.col('mrch_mcc').alias('mcc'), 'latitude', 'longitude', 'comp_mid', 'comp_lat', 'comp_lon', 'distance')
 
 print("{}:    ----- Join df_mid_comp_dist & df_mrch_tran -----".format(getDT()) )
 df_mid_comp_sales = df_mid_comp_dist.join(df_mrch_tran, ['comp_mid','mcc']) \
  .select('merchant_id', 'comp_mid','distance','sales', 'sales_cnt','mcc')
 
 print("{}:    ----- Write to {}.trade_area_compl_mcc_mid_dist_sales -----".format(getDT(), sDBName))
 df_mid_comp_sales.write.insertInto("{}.trade_area_compl_mcc_mid_dist_sales".format(sDBName), overwrite=bOverwrite )
 
 bOverwrite = False
 lowLimit = upperLimit+1
 upperLimit = lowLimit+iIncrements



print("{}:----- COMPLETE -----".format(getDT()) )


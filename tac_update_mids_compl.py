
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType,DoubleType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess

print("Input arguments:", sys.argv)
#Check arguments
if (len(sys.argv) != 6):
 print "Incorrect number of input arguments: Expected 5"
 print "1: db name (data_comm/vivid)"
 print "2: Job run date (Ex: 2020-09-15_17_13_54)"
 print "3: pass# (1 or 2 or 3)"
 print "4: extract_date for hisrtory table (Ex: 2020-09-15_17_13_54)"
 print "5: compl stage path (Ex: /lake/transform/worldpay/data_comm/trade_area_compl/trade_area_compl_mid_compl_stage"
 sys.exit(1)

sDBName = sys.argv[1]
s_job_run_date = sys.argv[2]
iPass = int(sys.argv[3])
s_ext_dt_hist = sys.argv[4]
s_compl_stage = sys.argv[5]


if iPass == 1:
 s_agg_table = sDBName+".truspnd_trade_area_compl_mid_agg"
elif iPass == 2:
 s_agg_table = sDBName+".truspnd_trade_area_compl_mid_agg_pass2"
elif iPass == 3:
 s_agg_table = sDBName+".truspnd_trade_area_compl_mid_agg_pass3"
else:
 print "ERROR: invalid input for Argument#3"
 print("1: {}\n2: {}\n3: {}\n4: {}\n5: {}".format(sDBName, s_job_run_date, iPass, s_ext_dt_hist, s_compl_stage) )
 sys.exit(1)



def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("{}:----- Arguments ----------".format(getDT()))
print("1: {}\n2: {}\n3: {}\n4: {}".format(sDBName, s_job_run_date, iPass, s_compl_stage) )

print("{}: getting spark session..".format(getDT()))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM TAC Merchant Market Share Pass1 Job PySpark") \
 .config("spark.executor.memoryOverhead", "4096") \
 .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
#.config("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#spark.conf.set(spark.yarn.executor.memoryOverhead","4096")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.shuffle.partitions", "25")

print("{}:----- {}.trade_area_compl_mid_compl".format(getDT(), sDBName) )
#(merchant_id, mrch_mcc, compl_at, last_run_dt, non_compl_reason, compl_pass,exec_stg)
df_mrch_compl = spark.table(sDBName+".trade_area_compl_mid_compl")

df_mrch_compl.cache()

print("{}:----- {} -----".format(getDT(), s_agg_table) )
#(mid, mcc, comp_count_5_miles, ..., )
df_mkt_share = spark.table(s_agg_table)

df_mkt_share.cache()

print("{}:   ----->current compliant mids".format(getDT()) )
df_mrch_curr = df_mkt_share.select('mid').distinct()

df_compl_at_curr = df_mkt_share.join(df_mrch_curr, ['mid']) \
 .withColumn('compl_at', F.when((F.col("comp_count_5_miles")>=4) & (F.col("mktshare_5_miles")<=0.30), 5) \
    .when((F.col("comp_count_10_miles")>=4) & (F.col("mktshare_10_miles")<=0.30), 10) \
    .when((F.col("comp_count_15_miles")>=4) & (F.col("mktshare_15_miles")<=0.30), 15) \
    .when((F.col("comp_count_20_miles")>=4) & (F.col("mktshare_20_miles")<=0.30), 20) \
    .when((F.col("comp_count_25_miles")>=4) & (F.col("mktshare_25_miles")<=0.30), 25) \
    .when((F.col("comp_count_30_miles")>=4) & (F.col("mktshare_30_miles")<=0.30), 30) \
    .when((F.col("comp_count_35_miles")>=4) & (F.col("mktshare_35_miles")<=0.30), 35) \
    .when((F.col("comp_count_40_miles")>=4) & (F.col("mktshare_40_miles")<=0.30), 40) \
    .when((F.col("comp_count_45_miles")>=4) & (F.col("mktshare_45_miles")<=0.30), 45) \
    .when((F.col("comp_count_50_miles")>=4) & (F.col("mktshare_50_miles")<=0.30), 50) \
    .when((F.col("comp_count_55_miles")>=4) & (F.col("mktshare_55_miles")<=0.30), 55) \
    .when((F.col("comp_count_60_miles")>=4) & (F.col("mktshare_60_miles")<=0.30), 60) \
    .when((F.col("comp_count_65_miles")>=4) & (F.col("mktshare_65_miles")<=0.30), 65) \
    .when((F.col("comp_count_70_miles")>=4) & (F.col("mktshare_70_miles")<=0.30), 70) \
    .when((F.col("comp_count_75_miles")>=4) & (F.col("mktshare_75_miles")<=0.30), 75) \
    .when((F.col("comp_count_80_miles")>=4) & (F.col("mktshare_80_miles")<=0.30), 80) \
    .when((F.col("comp_count_85_miles")>=4) & (F.col("mktshare_85_miles")<=0.30), 85) \
    .when((F.col("comp_count_90_miles")>=4) & (F.col("mktshare_90_miles")<=0.30), 90) \
    .when((F.col("comp_count_95_miles")>=4) & (F.col("mktshare_95_miles")<=0.30), 95) \
    .when((F.col("comp_count_100_miles")>=4) & (F.col("mktshare_100_miles")<=0.30), 100) \
  .otherwise(999999) ).select('mid', 'mcc','compl_at')

df_compl_curr = df_compl_at_curr.select(F.col('mid').alias('merchant_id'), F.col('mcc').alias('mrch_mcc'),'compl_at', F.when(F.col("compl_at")<999999, F.lit(s_job_run_date)).otherwise(F.lit(None)).alias('last_run_dt') \
   ,F.when(F.col("compl_at")<999999, F.lit(0)).otherwise(F.lit(9)).alias('non_compl_reason'), F.lit(iPass).alias('compl_pass') 
 )

print("{}:   ----->previous mids".format(getDT()) )
df_mrch_compl_prev = df_mrch_compl.select('merchant_id').subtract(df_mrch_curr)

df_compl_prev = df_mrch_compl.join(df_mrch_compl_prev, ['merchant_id']) \
 .select('merchant_id', 'mrch_mcc', 'compl_at', 'last_run_dt','non_compl_reason','compl_pass')

print("{}:----- Merge current & previous".format(getDT()) )
df_compl_merged = df_compl_curr.unionAll(df_compl_prev ) \
 .select('merchant_id', 'mrch_mcc', 'compl_at', 'last_run_dt','non_compl_reason','compl_pass', F.lit("pass "+str(iPass)+" - completed").alias('exec_stg') )

print("{}:   ---> Stage at: {}".format(getDT(), s_compl_stage) )
df_compl_merged.write.mode('overwrite').parquet(s_compl_stage)
#df_compl_merged.write.mode('overwrite').saveAsTable(sDBName + '.trade_area_compl_mid_compl')

print("{}:   ---> Read stage data from: {}".format(getDT(), s_compl_stage) )
df_compl_stg = spark.read.parquet(s_compl_stage)

df_compl_stg.cache()

print("{}:   ---> Write to {}.trade_area_compl_mid_compl".format(getDT(), sDBName))
df_compl_stg.write.insertInto("{}.trade_area_compl_mid_compl".format(sDBName), overwrite=True )

print("{}:   ---> Write to {}.trade_area_compl_mid_compl_hist".format(getDT(), sDBName))
df_compl_stg.withColumn('extract_date', F.lit(s_ext_dt_hist)).write.insertInto("{}.trade_area_compl_mid_compl_hist".format(sDBName), overwrite=True )

print("{}:----- COMPLETE -----".format(getDT()) )


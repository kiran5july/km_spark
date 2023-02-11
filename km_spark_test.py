from pyspark.sql import SparkSession
import pyspark.sql.functions as O
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess
from pyspark.sql.window import Window

#----- COMMENTS


print("Input arguments:", sys.argv)
#Check arguments
if (len(sys.argv) != 3):
 print "Incorrect number of input arguments: Expected 2"
 print "1: db name (kmdb)"
 print "2: HDFS application path (/hdfs/path)"
 sys.exit(1)

sDBName=sys.argv[1]
sPathAppRoot=sys.argv[2]

sDBName="db"
sPathAppRoot="/hdfs/km"

def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("{}:-----Arguments------------".format(getDT()))
print("1: {}\n2: {}\n".format(sDBName, sPathAppRoot) )

#print("{}:Tran dates: {} to {}".format(getDT(),sDateFromTranDt,sDateToTranDt))

print("{}: getting spark session..".format(getDT()))
print(dir(SparkSession.builder.config))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM Spark Test Job") \
 .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# .config("spark.executor.memoryOverhead", "4096")
#spark.conf.set("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
#spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.yarn.executor.memoryOverhead","8192")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
spark.conf.set("spark.unsafe.sorter.spill.read.ahead.enabled", "false")
#spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
spark.conf.set("spark.sql.parquet.columnarReaderBatchSiz","8192")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.sql.shuffle.partitions", "100")

#spark.conf.set("spark.memory.offHeap.enabled", "true")
#spark.conf.set("spark.memory.offHeap.size", "2G")


print("-----------------------------------------------")
spark_config_params = spark.sparkContext._conf.getAll()
df = spark.createDataFrame(spark_config_params).toDF('param','val')
#df.orderBy('param').show(1000, False)
df.select(O.substring(O.col('param'), 0, 60).alias('param'), O.substring(O.col('val'),0,60).alias('val')).orderBy('param').show(1000, False)

print("-----------------------------------------------")
spark_config_params = spark.sparkContext.getConf().getAll()
#for item in sorted(spark.sparkContext._conf.getAll()): print(item)
df = spark.createDataFrame(spark_config_params, ['param_name','param_val']).select(O.substring(O.col('param_name'), 0, 60).alias('param_name'), O.substring(O.col('param_val'),0,60).alias('param_val'))

df.orderBy('param_name').show(1000, False)
print("-----------------------------------------------")
lst_param_names=['spark.driver.memory','spark.executor.instances','spark.executor.memory','spark.executor.memoryOverhead','spark.executor.instances','spark.memory.fraction','spark.memory.storageFraction']
df.filter(O.col('param_name').isin(lst_param_names) ) \
 .orderBy('param_name').show(1000, False)

print("-----------------------------------------------")

df.filter(O.col('param_name').contains('memory') ) \
 .orderBy('param_name').show(1000, False)

print("-----------------------------------------------")


s_date_stage=datetime.now().strftime( '%Y_%m_%d_%H_%M_%S')
sPathStage = sPathAppRoot+"/km_stage_"+s_date_stage
sPathFinal = sPathAppRoot+"/final"


print("{}:----- data_comm.merchant_red -----".format(getDT()))
df_red = spark.table("kmdb.table") \
 .select(O.trim(O.col("i_chn")).alias("i_chn") )

lst_chns = df_red.select("i_chn").rdd.flatMap(lambda x: x).collect()

if len(lst_chns)<=0:
 print("{}:---> Red merchants list is blank, please check.".format(getDT()))
 sys.exit(0)

print("{}:    --> Red Chains: {}".format(getDT(), len(lst_chns)) )
#b_lst_chns = spark.sparkContext.broadcast(lst_chns)


print("{}:----- Stage at: {}".format(getDT(), sPathStage) )
df_red.write.mode('overwrite').parquet( sPathStage )

print("{}:----- Read from {}".format(getDT(), sPathStage))
df_final = spark.read.parquet(sPathStage)

iRecordsCount = df_final.count()
print("{}:   ---> Total records: {}".format(getDT(), iRecordsCount))

if iRecordsCount>0:
 print("{}:   ---> Writing to: {}".format(getDT(), sPathFinal ))
 df_final.coalesce(1 if iRecordsCount/12000000<=0 else iRecordsCount/12000000).write.mode('overwrite').option("sep","|").csv(sPathFinal, emptyValue='')
else:
 print("{}:----- No transactions to write. ".format(getDT()))
 #os.system("hdfs dfs -rm -r -skipTrash {}".format(sPathFinal))

#b_lst_chns.destroy()

print("{}----- COMPLETED -----".format(getDT()))

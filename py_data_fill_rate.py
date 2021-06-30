from pyspark.sql import SparkSession
import pyspark.sql.functions as O
#from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess
from decimal import *

#---COMMENTED


print("Input arguments:", sys.argv)
#Check arguments
if (len(sys.argv) != 3):
 print "Incorrect number of input arguments: Expected 2"
 print "1: table name (Ex: kmdb.table )"
 print "2: HDFS Path to save fill-rate (Ex: /lake/km)"
 sys.exit(1)

sTableName=sys.argv[1]
sPathRoot=sys.argv[2]

#sTableName="kmdb.table"
#sPathRoot="/lake/km"

def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("{}:-----Arguments------------".format(getDT()))
print("1: {}\n2: {}\n".format(sTableName, sPathRoot) )


print("{}: Getting spark session..".format(getDT()))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM Table Fill Rate Job App") \
 .getOrCreate()

#.config("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
#spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.yarn.executor.memoryOverhead","4096")
#spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.shuffle.partitions", "700")

df_tbl_data = spark.table(sTableName)
#df_tbl_data.cache()

i_row_count = df_tbl_data.count()

df_fill_counts = df_tbl_data.select([O.sum(O.when(O.col(c).isNull() | (O.col(c)==''), 1).otherwise(0)).alias(c) for c in df_tbl_data.columns])
#df_fill_counts.show(5)

d_fill_rate = map(lambda row: row.asDict(), df_fill_counts.collect())[0]

d_fill_prcnt = {k: str(round((Decimal(v)/Decimal(i_row_count))*100,4)) for k, v in d_fill_rate.items()}

print('Dictionary result:')
for k in d_fill_prcnt:
 print('  '+k+' -> '+str(d_fill_prcnt[k]) )

df_fill_prcnt = spark.createDataFrame(list(map(list, d_fill_prcnt.items())) , ['column', 'percent'])

df_fill_prcnt.show(10,False)

dt=datetime.now().strftime( '%Y%m%d_%H%M%S')
df_fill_prcnt.coalesce(1).write.mode('overwrite').options(header='true').csv(sPathRoot+"/"+sTableName+"_{}".format(dt), emptyValue='')
#.options("sep"="|",quoteAll='true')


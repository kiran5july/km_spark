'''
#---------Spark-submit-------
spark2-submit --name "KM Save Table as CSV"       --master yarn --deploy-mode client  \
     --driver-class-path /etc/spark2/conf:/etc/hive/conf \
     --num-executors 15       --executor-cores 5       --executor-memory 15G  \
     --queue general       --conf spark.ui.port=44455       --conf spark.port.maxRetries=100 \
     --conf spark.unsafe.sorter.spill.read.ahead.enabled=false \
    py_save_table_as_csv.py "kmdb.table" "/hdfs/path/" ","

'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as O
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess
from decimal import *

print("Input arguments:", sys.argv)
if (len(sys.argv) != 4):
 print "Incorrect number of input arguments: Expected 2"
 print "1: table name (kmdb.table)"
 print "2: Result Save Path in HDFS (/hdfs/path/)"
 print "3: Delimiter (, or |)"
 sys.exit(1)

s_table_name = sys.argv[1]
s_path_save = sys.argv[2]
s_delimiter =  sys.argv[3]

s_datetime_for_temp_staging = datetime.now().strftime( '%Y_%m_%d_%H_%M')
path_sep = "" if s_path_save[-1]=='/' else "/"
file_name_prefix = 'data_'+s_table_name+"_"+s_datetime_for_temp_staging
file_name = file_name_prefix+".csv"
temp_hdfs_path = s_path_save + path_sep + file_name_prefix

def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("\n{}:-----Arguments------------".format(getDT()))
print("1: {}\n2: {}\n3 {}\n".format(s_table_name, s_path_save, s_delimiter) )


print("{}: getting spark session..".format(getDT()))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM Save Table as CSV Job PySpark") \
 .config("spark.executor.memoryOverhead", "4096") \
 .getOrCreate()

#.config("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
#spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#spark.conf.set(spark.yarn.executor.memoryOverhead","4096")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")
#spark.conf.set("spark.io.compression.codec", "snappy")
#spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


print("{}:----- Get columns".format(getDT()) )
df_tbl_data = spark.table(s_table_name)
df_tbl_data.cache()
row_count = df_tbl_data.count()

cols = df_tbl_data.columns

header_string = s_delimiter.join(cols)

row_count = df_tbl_data.count()
if row_count <= 0:
 print("{}:ERROR ---> No data in table".format(getDT()))
 #sys.exit(0)

df_tbl_data.coalesce(1).write.option("sep",s_delimiter).mode('overwrite').csv(temp_hdfs_path, emptyValue='')

print('{}:Saving to file: '.format(getDT()) + '/tmp/'+file_name )
import os
os.system('echo "'+header_string+'" > /tmp/'+file_name)
os.system('hdfs dfs -cat {}/* >> {}'.format(temp_hdfs_path, '/tmp/'+file_name) )

print('{}:Saving to HDFS: '.format(getDT()) + s_path_save+path_sep+file_name )
os.system('hdfs dfs -put -f '+'/tmp/'+file_name + ' ' + s_path_save+path_sep)

os.system('hdfs dfs -rm -r '+temp_hdfs_path)

print("{}:----- COMPLETED ----".format(getDT()))


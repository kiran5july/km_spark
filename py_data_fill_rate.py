'''
#---------Spark-submit-------
spark2-submit --name "KM Table fill rate"       --master yarn --deploy-mode client  \
     --driver-class-path /etc/spark2/conf:/etc/hive/conf \
     --num-executors 15       --executor-cores 5       --executor-memory 15G  \
     --queue general       --conf spark.ui.port=44455       --conf spark.port.maxRetries=100 \
     --conf spark.unsafe.sorter.spill.read.ahead.enabled=false \
    py_table_fill_rates.py "kmdb.table" "/hdfs/path/"

'''
from pyspark.sql import SparkSession
import pyspark.sql.functions as O
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess
from decimal import *

print("Input arguments:", sys.argv)
if (len(sys.argv) != 3):
 print "Incorrect number of input arguments: Expected 2"
 print "1: table name (kmdb.table)"
 print "2: Result Save Path in HDFS (/hdfs/path/)"
 sys.exit(1)

s_table_name = sys.argv[1]
s_path_save = sys.argv[2]

s_datetime_for_temp_staging = datetime.now().strftime( '%Y_%m_%d_%H_%M')
path_sep = "" if s_path_save[-1]=='/' else "/"
file_name_prefix = 'table_fill_rates_'+s_table_name+"_"+s_datetime_for_temp_staging
file_name = file_name_prefix+".csv"
temp_hdfs_path = s_path_save + path_sep + file_name_prefix

def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("\n{}:-----Arguments------------".format(getDT()))
print("1: {}\n2: {}\n".format(s_table_name, s_path_save) )


print("{}: getting spark session..".format(getDT()))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM Table fillrate Job PySpark") \
 .config("spark.executor.memoryOverhead", "4096") \
 .getOrCreate()

#.config("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
#spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#spark.conf.set(spark.yarn.executor.memoryOverhead","4096")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


print("{}:----- Get columns".format(getDT()) )
df_tbl_data = spark.table(s_table_name)
df_tbl_data.cache()
row_count = df_tbl_data.count()

cols = df_tbl_data.columns

df_fill_counts = df_tbl_data.select([O.sum(O.when(O.col(c).isNull() | (O.col(c)==''), 1).otherwise(0)).alias(c) for c in cols])

lst_final_result = [ ('', '', s_table_name, '', ''),
                     ('LINE#', 'COLUMN NAME', 'BLANK ROWS', 'TOTAL ROWS', '%')
                   ]
dict_fill_rate = map(lambda row: row.asDict(), df_fill_counts.collect())[0]

count = 1
for itm in dict_fill_rate.items():
 lst_final_result.append((str(count), str(itm[0]), str(itm[1]), str(row_count), str(round( (Decimal(row_count)-Decimal(itm[1]))*100/row_count ,3))) )
 count+=1



if row_count <= 0:
 print("{}:ERROR ---> No data in table".format(getDT()))
 sys.exit(0)

df_result = spark.createDataFrame(lst_final_result)

df_result.coalesce(1).write.option("sep",",").mode('overwrite').csv(temp_hdfs_path, emptyValue='')

print('{}:Saving to file: '.format(getDT()) + '/tmp/'+file_name )
os.system('hdfs dfs -cat {}/* > {}'.format(temp_hdfs_path, '/tmp/'+file_name) )

print('{}:Saving to HDFS: '.format(getDT()) + s_path_save+path_sep+file_name )
os.system('hdfs dfs -put -f '+'/tmp/'+file_name + ' ' + s_path_save+path_sep)

os.system('hdfs dfs -rm -r '+temp_hdfs_path)

print("{}:----- COMPLETED ----".format(getDT()))




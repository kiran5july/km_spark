'''
#---------Spark-submit-------
spark2-submit --name "KM Hive Tables Directories"       --master yarn --deploy-mode client  \
     --driver-class-path /etc/spark2/conf:/etc/hive/conf \
     --num-executors 15       --executor-cores 5       --executor-memory 15G  \
     --queue general       --conf spark.ui.port=44455       --conf spark.port.maxRetries=100 \
     --conf spark.unsafe.sorter.spill.read.ahead.enabled=false \
    py_view_hive_directories.py "kmdb" "km_tbl" "/hdfs/km/"
'''

from pyspark.sql import SparkSession
import pyspark.sql.functions as O
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess
from decimal import *

print("Input arguments:", sys.argv)
if (len(sys.argv) != 4):
 print "Incorrect number of input arguments: Expected 3"
 print "1: database name (kmdb)"
 print "2: Table Name Search String (km_tbl)"
 print "3: Result Save Path in HDFS (/hdfs/path/)"
 sys.exit(1)

s_db_name = sys.argv[1]
s_table_name = sys.argv[2]
s_path_save = sys.argv[3]

#s_db_name = "kmdb"
#s_table_name = ""
#s_path_save = "/hdfs/km/"


s_datetime_for_temp_staging = datetime.now().strftime( '%Y_%m_%d_%H_%M')
path_sep = "" if s_path_save[-1]=='/' else "/"
file_name_prefix = 'table_hive_dir_paths_'+s_table_name+"_"+s_datetime_for_temp_staging
file_name = file_name_prefix+".csv"
temp_hdfs_path = s_path_save + path_sep + file_name_prefix

def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

print("\n{}:-----Arguments------------".format(getDT()))
print("1: {}\n2: {}\n3: {}\n".format(s_db_name, s_table_name, s_path_save) )


print("{}: getting spark session..".format(getDT()))

spark = SparkSession \
 .builder \
 .enableHiveSupport() \
 .appName("KM Hive Table Directories List Job PySpark") \
 .config("spark.executor.memoryOverhead", "4096") \
 .getOrCreate()

#.config("yarn.nodemanager.vmem-check-enabled", False) --- Giving error
#spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
#spark.conf.set(spark.yarn.executor.memoryOverhead","4096")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
#spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

print("{}:----- Get all tables".format(getDT()) )
df_tbls = spark.sql("show tables in "+s_db_name)

if s_table_name!='':
  df_tbls = spark.sql("show tables in "+s_db_name).filter(O.col("tablename").contains(s_table_name))
else:
  df_tbls = spark.sql("show tables in "+s_db_name)


df_tbls_2 = df_tbls.withColumn("table_name", O.concat(O.col("database"), O.lit("."), O.col("tableName")) ) \
 .drop("isTemporary", "database", "tableName")

#------store in dictionary: {'table':['loc'], ... }
#dict_tables = df_tbls_2.select('table_name', O.split(O.lit(''),',')).rdd.collectAsMap()
##for k,v in dict_tables.items(): print(k+" -> "+str(v))
#for k,v in dict_tables.items():
# df_tbl_loc = spark.sql("describe formatted "+k).filter(O.col("col_name")=="Location").drop("col_name", "comment")
# s_loc = df_tbl_loc.rdd.take(1)[0][0]
# dict_tables[k][0]=s_loc

#for k,v in dict_tables.items(): print(k+" -> "+v[0])


#------store in list: [('table_name','loc'),...]

list_tables = df_tbls_2.select('table_name').rdd.flatMap(lambda x: x).collect()

if len(list_tables) <= 0:
 print("{}:ERROR ---> No results with specified criteria".format(getDT()))
 sys.exit(0)

list_of_recs = []

for tbl in list_tables:
 s_loc = spark.sql("describe formatted "+tbl).filter(O.col("col_name")=="Location").drop("col_name", "comment").rdd.take(1)[0][0]
 list_of_recs.append((tbl, s_loc))


df_result = spark.createDataFrame(list_of_recs).toDF('table_name', 'location')

df_result.coalesce(1).write.options(sep='|', header='true').mode('overwrite').csv(temp_hdfs_path, emptyValue='')

print('{}:Saving to file: '.format(getDT()) + '/tmp/'+file_name )
os.system('hdfs dfs -cat {}/* > {}'.format(temp_hdfs_path, '/tmp/'+file_name) )

print('{}:Saving to HDFS: '.format(getDT()) + s_path_save+path_sep+file_name )
os.system('hdfs dfs -put -f '+'/tmp/'+file_name + ' ' + s_path_save+path_sep)

os.system('hdfs dfs -rm -r '+temp_hdfs_path)

print("{}:----- COMPLETED ----".format(getDT()))

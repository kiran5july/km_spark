
from pyspark.sql import SparkSession
import pyspark.sql.functions as O
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType
from datetime import datetime, date, timedelta
import os, sys, traceback, subprocess


sDBName="kmdb"
sPathOrcl="/bda/app"
lMaxPartitions = spark.sql("SHOW PARTITIONS {}.km_dly".format(sDBName)) \
 .select( O.regexp_extract(O.col('partition'), 'source_system=([\w]+)',1).alias('source_system'), O.regexp_extract(O.col('partition'), 'source_system=([\w]+)/extract_date=([\S]+)', 2).alias('extract_date') ) \
 .groupBy('source_system').agg({"extract_date": "max"}).select(O.concat(O.col('source_system'),O.lit(':'),O.col('max(extract_date)'))).rdd.flatMap(lambda x: x).collect()

if len(lMaxPartitions)<2:
 print("{}: ONLY 1 type if fetched: {}".format(getDT()), lMaxPartitions)
 sys.exit(100)

sPIN = "".join([k for k in lMaxPartitions if k.startswith('PIN')]).split(":")[1]
print("{}:   --> PIN: {}".format(getDT(), sPIN))
sPINFile = sPathOrcl+"/km_archive/km_pin_max.txt"
os.system("echo \"{}\" | hdfs dfs -put -f - {}".format(sPIN, sPINFile))




#---------------reading & compare with current date in shell script
hdfsArchive="$sPathOrcl/km_archive"
pin_stl_date=$(hdfs dfs -cat ${hdfsArchive}/km_pin_max.txt)
if [[ "$pin_stl_date" < "$(date +'%Y-%m-%d')" ]]; then
  echo "   --> PIN Stlmnt table date is less, so use this date: $pin_stl_date";
  echo "$pin_stl_date" | hdfs dfs -put -f - ${hdfsArchive}/km_pin_date.txt
else
  echo "   --> Update today's date for PIN";
  echo "$(date +'%Y-%m-%d')" | hdfs dfs -put -f - ${hdfsArchive}/km_pin_date.txt
fi
#-------------------------------------------------------------------

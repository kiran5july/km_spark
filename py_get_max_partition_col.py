import pyspark.sql.functions as F
from datetime import datetime, date, timedelta



#Using table metadata 
sMaxPartition = spark.sql("SHOW PARTITIONS kmdb.tbl1").select('partition').agg({"partition": "max"}).collect()[0][0]
#sMaxPartition = spark.sql("SHOW PARTITIONS kmdb.tbl1").select('partition').rdd.max()[0]
#max_mrch_mast = spark.sql("SHOW PARTITIONS kmdb.tbl1").agg(F.max("partition")).collect()[0][0].replace("extract_date=","")

if sMaxPartition==None:
 print("{}: No partition fetched".format(getDT()))
 sys.exit(100)

iStartIndex = sMaxPartition.find('=')
if iStartIndex<0:
 print("{}: No partition fetched".format(getDT()))
 sys.exit(100)
 
sMaxDate = sMaxPartition[iStartIndex+1:]


#Using querying data within last 36 days & get max
sDBName="kmdb"
mx_ext_mrch=spark.table("{}.kmtbl".format(sDBpdw)).select("extract_date") \
 .filter(F.col("extract_date")>=(date.today()-timedelta(36)).strftime('%Y-%m-%d') ) \
 .agg(F.max("extract_date")) \
 .collect()[0][0]
 

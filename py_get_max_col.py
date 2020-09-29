import pyspark.sql.functions as F
from datetime import datetime, date, timedelta



#Using table metadata 
sMaxPartition = spark.sql("SHOW PARTITIONS demographics_db.tef_mrchnt_info").select('partition').agg({"partition": "max"}).collect()[0][0]
#sMaxPartition = spark.sql("SHOW PARTITIONS demographics_db.tef_mrchnt_info").select('partition').rdd.max()[0]
#max_mrch_mast = spark.sql("SHOW PARTITIONS pdw.tbc_mrchnt_mast_bo").agg(F.max("partition")).collect()[0][0].replace("extract_date=","")

iStartIndex = sMaxPartition.find('=')
if iStartIndex<0:
 print("{}: No partition fetched".format(getDT()))
 sys.exit(100)
 
sMaxDate = sMaxPartition[iStartIndex+1:]


#Using querying data & get max
sDBName="pdw"
mx_ext_mrch=spark.table("{}.mrch_mast_bin".format(sDBpdw)).select("extract_date") \
 .filter(F.col("extract_date")>=(date.today()-timedelta(36)).strftime('%Y-%m-%d') ) \
 .agg(F.max("extract_date")) \
 .collect()[0][0]
 

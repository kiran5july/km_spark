

#-----Sample data:
Format: [5 digit number code][date in format yyyy-mm-dd][price]
dataMerged=spark.createDataFrame(["100012020-01-0199.99", "100022020-01-038.0", "100032020-01-035.7"], StringType()) \
 .toDF("all_col")

dataMerged.show(10,False)
+--------------------+
|all_col             |
+--------------------+
|100012020-01-0199.99|
|100022020-01-038.0  |
|100032020-01-035.7  |
+--------------------+

#---Expected output:
+--------------------+---------+----------+----------+
|             all_col|part_code|     op_dt|part_price|
+--------------------+---------+----------+----------+
|100012020-01-0199.99|    10001|2020-01-01|     99.99|
|  100022020-01-038.0|    10002|2020-01-03|       8.0|
|  100032020-01-035.7|    10003|2020-01-03|       5.7|
+--------------------+---------+----------+----------+


#Reg Expressions:
rex=r'([\w\W]{5})([\w\W]{10})([\w\W]+)'
rex=r'([0-9]{5})([0-9-]{10})([0-9.]+)'
rex=r'([0-9]{5})([0-9]{4}-[0-9]{2}-[0-9]{2})([0-9.]+)'


#using regexp_extract
import pyspark.sql.functions as F
dataMerged.withColumn("part_code", F.regexp_extract(F.col('all_col'), rex,1) ) \
 .withColumn("op_dt", F.regexp_extract(F.col('all_col'), rex,2) ) \
 .withColumn("part_price", F.regexp_extract(F.col('all_col'), rex,3) ).show(5)

+--------------------+---------+----------+----------+
|             all_col|part_code|     op_dt|part_price|
+--------------------+---------+----------+----------+
|100012020-01-0199.99|    10001|2020-01-01|     99.99|
|  100022020-01-038.0|    10002|2020-01-03|       8.0|
|  100032020-01-035.7|    10003|2020-01-03|       5.7|
+--------------------+---------+----------+----------+


#---using python's re functions
#String pattern matching methods:
-findall() -> returns tuples
#re.findall(rex, "100012020-01-0199.99")[0] -> ('10001', '2020-01-01', '99.99')
-search() -> returns tuples
#re.search(rex, "100012020-01-0199.99").groups()  -> ('10001', '2020-01-01', '99.99')

import re
def breakString(s):
 return list(re.findall(rex, s)[0])

from pyspark.sql.types import StringType,ArrayType
udfRecMerged=F.udf(breakString, ArrayType(StringType()))

dataMerged.withColumn("data_list", udfRecMerged(F.col("all_col"))) \
 .withColumn("part_code", F.col("data_list")[0] ) \
 .withColumn("op_dt", F.col("data_list")[1] ) \
 .withColumn("part_price", F.col("data_list")[2] ).show(5)

+--------------------+--------------------+---------+----------+----------+
|             all_col|          rec_merged|part_code|     op_dt|part_price|
+--------------------+--------------------+---------+----------+----------+
|100012020-01-0199.99|10001,2020-01-01,...|    10001|2020-01-01|     99.99|
|  100022020-01-038.0|10002,2020-01-03,8.0|    10002|2020-01-03|       8.0|
|  100032020-01-035.7|10003,2020-01-03,5.7|    10003|2020-01-03|       5.7|
+--------------------+--------------------+---------+----------+----------+


#----UDF return as string
def breakString(s):
 rec=re.findall(rex, s)[0]
 return rec[0]+","+rec[1]+","+rec[2]

#other way for function: findall() to get tuple, convert to list & join all elements by comma
def breakString(s):
 return ( ','.join( list(re.findall(rex, s)[0] ) )  )

from pyspark.sql.types import StringType,ArrayType
udfRecMerged=F.udf(breakString, StringType())

dataMerged.withColumn("rec_merged", udfRecMerged(F.col("all_col"))) \
 .withColumn("part_code", F.split(F.col("rec_merged"), ",")[0] ) \
 .withColumn("op_dt", F.split(F.col("rec_merged"), ",")[1] ) \
 .withColumn("part_price", F.split(F.col("rec_merged"), ",")[2] ).show()



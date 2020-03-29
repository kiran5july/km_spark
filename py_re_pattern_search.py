

Sample data:
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

String pattern matching methods:
-findall() -> returns tuples
-search() -> returns tuples


def breakString(s):
 rec=re.findall(r"([0-9]{5})([0-9-]{10})([0-9.]+)",s)[0]
 return rec[0]+","+rec[1]+","+rec[2]

#other way for function: findall() to get tuple, convert to list & join all elements by comma
def breakString(s):
 return ( ','.join( list(re.findall(r"([0-9]{5})([0-9-]{10})([0-9.]+)", s)[0] ) )  )
 
#Can use search():
#re.search(r"([0-9]{5})([0-9-]{10})([0-9.]+)", "100012020-01-0199.99").groups()
#re.findall(r"([0-9]{5})([0-9]{4}-[0-9]{2}-[0-9]{2})([0-9.]+)", "100012020-01-0199.99")[0]

udfRecMerged=F.udf(breakString, StringType())

dataMerged.withColumn("rec_merged", udfRecMerged(F.col("all_col"))) \
 .withColumn("part_code", F.split(F.col("rec_merged"), ",")[0] ) \
 .withColumn("op_dt", F.split(F.col("rec_merged"), ",")[1] ) \
 .withColumn("part_price", F.split(F.col("rec_merged"), ",")[2] ).show()

+--------------------+--------------------+---------+----------+----------+
|             all_col|          rec_merged|part_code|     op_dt|part_price|
+--------------------+--------------------+---------+----------+----------+
|100012020-01-0199.99|10001,2020-01-01,...|    10001|2020-01-01|     99.99|
|  100022020-01-038.0|10002,2020-01-03,8.0|    10002|2020-01-03|       8.0|
|  100032020-01-035.7|10003,2020-01-03,5.7|    10003|2020-01-03|       5.7|
+--------------------+--------------------+---------+----------+----------+





#Description:
#--How to read all hidden columns


#Create Parquet hive table
CREATE EXTERNAL TABLE kmdb.test_ext_parquet (id STRING, name STRING) STORED AS PARQUET LOCATION '/km_hadoop/data/test_ext_parquet';


#--write as columns(id,name)
dataDF1=spark.createDataFrame([("1", "aaa"), ("2", "bbb")]) \
 .toDF("id", "name")
dataDF1.show(200, False)
dataDF1.coalesce(1).write.mode('append').parquet("/km_hadoop/data/test_ext_parquet")


#--write as columns(id2,name2)
dataDF2=spark.createDataFrame([("101", "ggg"), ("102", "hhh")]) \
 .toDF("id2", "name2")
dataDF2.show(200, False)
dataDF2.coalesce(1).write.mode('append').parquet("/km_hadoop/data/test_ext_parquet")


#--write as columns(id,name2)
dataDF3=spark.createDataFrame([("201", "xxx"), ("202", "yyy")]) \
 .toDF("id", "name2")
dataDF3.show(200, False)
dataDF3.coalesce(1).write.mode('append').parquet("/km_hadoop/data/test_ext_parquet")



#Read all columns
>>> spark.read.option("mergeSchema", "true").parquet("/km_hadoop/data/test_ext_parquet").show(10,False)
+----+-----+----+----+
|id2 |name2|id  |name|
+----+-----+----+----+
|101 |ggg  |null|null|
|102 |hhh  |null|null|
|null|xxx  |201 |null|
|null|yyy  |202 |null|
|null|null |1   |aaa |
|null|null |2   |bbb |
+----+-----+----+----+

#Current Hive table doesn't show all data, so create new table with all column names.
CREATE EXTERNAL TABLE kmdb.test_ext_parquet_all_columns (
  id STRING, name STRING,id2 STRING, name2 STRING)
   STORED AS PARQUET LOCATION '/km_hadoop/data/test_ext_parquet';


#Read hidden columns using custom Schema in PySpark

from pyspark.sql.types import StringType, StructType, StructField

mySchema = StructType([
 StructField("id", StringType(), True),
 StructField("name", StringType(), True),
 StructField("id2", StringType(), True),
 StructField("name2", StringType(), True)])

spark.read.schema(mySchema).parquet("/km_hadoop/data/test_ext_parquet").show(10,False)
+----+----+----+-----+
|id  |name|id2 |name2|
+----+----+----+-----+
|null|null|101 |ggg  |
|null|null|102 |hhh  |
|201 |null|null|xxx  |
|202 |null|null|yyy  |
|1   |aaa |null|null |
|2   |bbb |null|null |
+----+----+----+-----+

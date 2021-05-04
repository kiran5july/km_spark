
from pyspark.sql.types import StringType

spark.createDataFrame(["a", "b", "c", "d", "e"], StringType()) \
 .toDF("id").show()


spark.createDataFrame([("a", 10), ("b", 10), ("c", 7), ("d", 16)]) \
 .toDF("id", "rec_count") \
 .show(200, False)
 
spark.createDataFrame([("a", 10), ("b", 10), ("c", 7), ("d", 16)], ["id", "rec_count"]) \
 .show(10, False)

spark.createDataFrame([("111")], StringType()) \
  .toDF("id").show()




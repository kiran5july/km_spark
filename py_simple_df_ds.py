

spark.createDataFrame([("a", 10), ("b", 10), ("c", 7), ("d", 16)]) \
 .toDF("id", "rec_count") \
 .show(200, False)
 
spark.createDataFrame([("a", 10), ("b", 10), ("c", 7), ("d", 16)], ["id", "rec_count"]) \
 .show(10, False)

spark.createDataFrame([("111")], StringType()) \
  .toDF("id").show()

spark.createDataFrame(["100012020-01-0199.99", "10022020-01-038.0", "10032020-01-035.7"], StringType()) \
 .toDF("all_col").show()


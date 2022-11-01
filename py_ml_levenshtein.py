

#------levenshten distance

from pyspark.sql import functions as F
#from pyspark.sql.functions import col, length, greatest, levenshtein

df = spark.createDataFrame([("jane", "jack"), ("kris", "krish"), ("hello", "hello") ]).toDF("name1", "name2")
df.withColumn('conf_level', F.levenshtein('name1', 'name2') / F.greatest(F.length(col('name1')), F.length(F.col('name2')))).show(5)


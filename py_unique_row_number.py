


------Get unique row# to each record
df_data = spark.createDataFrame([("a", 10), ("b", 10), ("c", 7), ("d", 16)]) \
 .toDF("id", "rec_count")


#1: Using row_number(window)
from pyspark.sql.window import Window
winRN = Window.orderBy(F.monotonically_increasing_id())

import pyspark.sql.functions as F
df_data.withColumn("rn", F.row_number().over(winRN) ).show(5,False)

#ERROR WITH LARGE DATASET: WARN window.WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.


#2: Using rdd's zipWithIndex()
df_data.rdd.zipWithIndex().map(lambda x: (x[0][0], x[0][1], x[1]+1)).toDF(df_data.columns+["rn"]).show(5)


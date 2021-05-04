


------Get unique row# to each record
#ISSUE: ERROR WITH LARGE DATASET
# WARN window.WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.

df_data = spark.createDataFrame([("a", 10), ("b", 10), ("c", 7), ("d", 16)]) \
 .toDF("id", "rec_count")


#1: Using row_number().over(window)
#window requires an orderBy column
from pyspark.sql import Window

w_id = Window.orderBy(F.col('id'))
df_data.withColumn('unique_no', F.row_number().over(w_id)).show(10)



#NOT BETTER THAN #1: using a fake partition column
df2 = df_data.withColumn('fake_col', F.lit('0'))

from pyspark.sql import Window
w_id = Window.partitionBy('fake_col').orderBy(F.col('id'))
df2.withColumn('unique_no', F.row_number().over(w_id)).show(10)



#WORKS BUT NOT BETTER: Generate a unique column & use it with row_number()
from pyspark.sql.window import Window
winMID = Window.orderBy(F.monotonically_increasing_id())

import pyspark.sql.functions as F
df_data.withColumn("rn", F.row_number().over(winMID) ).show(5,False)




#2: Using rdd's zipWithIndex()
df_data.rdd.zipWithIndex().map(lambda x: (x[0][0], x[0][1], x[1]+1)).toDF(df_data.columns+["rn"]).show(5)


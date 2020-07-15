


df_amt=spark.createDataFrame([("a", "10.923"), ("b", "2.5522"), ("c", "16.1")]).toDF("id", "amount")

import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DecimalType,DoubleType

df_amt.select(F.col("amount").cast(DoubleType()).alias("amt_dbl")
             ,F.col("amount").cast(DecimalType()).alias("amt_dec")
             ,F.col("amount").cast(FloatType()).alias("amt_flt")
             ,F.col("amount").cast(IntegerType()).alias("amt_int")
             ) \
 .show()

+-------+-------+-------+-------+
|amt_dbl|amt_dec|amt_flt|amt_int|
+-------+-------+-------+-------+
| 10.923|     11| 10.923|     10|
| 2.5522|      3| 2.5522|      2|
|   16.1|     16|   16.1|     16|
+-------+-------+-------+-------+


#--- to 2 decimal precision
df_amt.select(F.col("amount").cast(DecimalType(15,2)).alias("amt_dec")).show()
+-------+
|amt_dec|
+-------+
|  10.92|
|   2.55|
|  16.10|
+-------+



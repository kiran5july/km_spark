

from datetime import datetime
import pyspark.sql.functions as F
from pyspark.sql.types import StringType


def DateStringChangeFormat(date_str):
  format_from = '%m/%d/%Y'
  format_to = '%Y-%m-%d'
  return datetime.strptime(date_str, format_from).strftime(format_to)
#test: DateStringChangeFormat('12/31/2021' )


#spark.udf.register("DateStringChangeFormat", DateStringChangeFormat, StringType())
DateStringChangeFormat = F.udf(lambda x: DateStringChangeFormat(x), StringType())

#test on DF
df_hist = spark.createDataFrame(['12/31/2021'], StringType()).toDF('tr_date')
df_hist.withColumn('tr_date2', DateStringChangeFormat(F.col('tr_date'))).show(5)


#---UDF with multiple arguments
def DateStringChangeFormat3(date_str, from_format, to_format):
  return datetime.strptime(date_str, from_format).strftime(to_format)
#test: DateStringChangeFormat('12/31/2021', '%m/%d/%Y', '%Y-%m-%d')

DateStringChangeFormat3 = F.udf(lambda x: DateStringChangeFormat3(x[0],x[1],x[2]), StringType())

#test on DF
df_hist.withColumn('tr_date2', DateStringChangeFormat3(F.col('tr_date'), F.lit('%m/%d/%Y'), F.lit('%Y-%m-%d')).show(5)



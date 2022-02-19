
#---UDF with single column input
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

def DateStringChangeFormat(date_str):
  from datetime import datetime
  format_from = '%m/%d/%Y'
  format_to = '%Y-%m-%d'
  return datetime.strptime(date_str, format_from).strftime(format_to)
#test: DateStringChangeFormat('12/31/2021' )


udf_DateStringChangeFormat = F.udf(DateStringChangeFormat, StringType())

#test on DF
df_hist = spark.createDataFrame(['12/31/2021'], StringType()).toDF('tr_date')
df_hist.withColumn('tr_date2', udf_DateStringChangeFormat(F.col('tr_date'))).show(5)
+----------+----------+
|   tr_date|  tr_date2|
+----------+----------+
|12/31/2021|2021-12-31|
+----------+----------+


#---UDF with multiple arguments (formats as input arguments)
def DateStringChangeFormat3(date_str, from_format, to_format):
  from datetime import datetime
  return datetime.strptime(date_str, from_format).strftime(to_format)
#test: DateStringChangeFormat3('12/31/2021', '%m/%d/%Y', '%Y-%m-%d')

udf_DateStringChangeFormat3 = F.udf(DateStringChangeFormat3, StringType())

#test on DF
df_hist.withColumn('tr_date2', udf_DateStringChangeFormat3(F.col('tr_date'), F.lit('%m/%d/%Y'), F.lit('%Y-%m-%d'))).show(5)
+----------+----------+
|   tr_date|  tr_date2|
+----------+----------+
|12/31/2021|2021-12-31|
+----------+----------+



--Create table for output
-TABLE
CREATE EXTERNAL TABLE kmdb.tbl2
 LIKE kmdb.tbl2
 LOCATION '/kmdb/tbl2';

--Get sample data from kmdb.tbl1 and copy into vivid.tac_random_mids
import pyspark.sql.functions as F
dfMrch = spark.table("kmdb.tbl1") \
 .filter((F.col("merchant_id").isNotNull()) & (F.trim(F.col("merchant_id"))!="") & (F.col("customer_level")=='MT') & (F.col("portfolio")!='ECOM') ) \
 .select("merchant_id","customer_mcc")
dfMrch.cache()

dfNewMids = (dfMrch.sample(withReplacement=False, fraction=(1.08*float(35000)/dfMrch.count()), seed=111).limit(35000) )
dfNewMids.select('merchant_id','customer_mcc') \
 .write.mode('overwrite').insertInto("kmdb.tbl2", overwrite=True )

#Using a function
def get_counted_sample(df, row_count, withReplacement=False, seed=113170):
 fraction = 1.08 * float(row_count) / df.count()
 
 if fraction>1.0:
  fraction = 1.0
 
 result_df = (df
  .sample(with_replacement, ratio, seed)
  .limit(row_count)
  )
 return result_df

#Method call
dfNewMids = get_counted_sample(dfMrch, 35000, False, 111)



#--- manual sample
#--Logic: Add row_number() to each record & pull specific number of records
#1: Using row_number(window)
from pyspark.sql.window import Window
winRN = Window.orderBy(F.monotonically_increasing_id())

dfMrch.withColumn("rn", F.row_number().over(winRN) ).filter(F.col("rn")<=35000).select('merchant_id','customer_mcc').count()

ERROR: WARN window.WindowExec: No Partition Defined for Window operation! Moving all data to a single partition, this can cause serious performance degradation.

#2: Using rdd's zipWithIndex()
dfMrch.rdd.zipWithIndex().map(lambda x: (x[0][0], x[0][1], x[1]+1)).toDF(df_data.columns+["rn"]).select('merchant_id','customer_mcc').show(5)

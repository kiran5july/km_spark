
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


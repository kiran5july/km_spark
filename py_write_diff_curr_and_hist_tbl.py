
#Steps
#-get current
#-get from hist
#-det difference
#-Broadcast if found
#-join to originl table
#-join with a master table
#-write new mrch to a csv file



import pyspark.sql.functions as O
from datetime import datetime, date, timedelta
#dt=datetime.now().strftime( '%Y%m%d_%H%M%S')

dfMrchList = spark.table("km.mrch_list")

df_mrch_hist = spark.table("km.mrch_list_hist")

sMaxPartition=spark.sql("SHOW PARTITIONS km.mrch_list_hist").select('partition').agg({"partition": "max"}).collect()[0][0]
mx_extr_mrch_inf = sMaxPartition[sMaxPartition.find('=')+1:]

df_mrch_hist_previous = df_mrch_hist.filter(O.col('extract_date')==mx_extr_mrch_inf)

#new mids
df_new_mids = dfMrchList.select('km_mid').subtract(df_mrch_hist_previous.select('km_mid'))

lst_merchants = df_new_mids.select("km_mid").rdd.flatMap(lambda x: x).collect()
if len(lst_merchants)>0:
 print(" --> Merchants list: {}".format(lst_merchants) )
 b_lst_merchants = spark.sparkContext.broadcast(lst_merchants)
else:
 print(" **** NO NEW MERCHANTS ***")


df_mtch_today = dfMrchList.filter(O.col("km_mid").isin(b_lst_merchants.value)) \
 .select('loc_id1', 'customer_name','address','locality','region','km_cust_name', 'km_address', 'km_city', 'km_state', 'km_mid')

 #.withColumn("mid_sha", O.sha2(O.concat(O.col("km_mid"), O.lit("ug2M7VUg9EBQku7G")), 256))

df_mtch_today.show(10) 

dfMrch = spark.table("km.mrch_master").select("merchant_id","merchant_name","physical_address_1","physical_city","physical_state","customer_status")
dfMrch.cache()

dfMrchMtchFinal_td = df_mtch_today.join(dfMrch, (df_mtch_today.km_mid==dfMrch.merchant_id), "left_outer" ) \
 .select('loc_id1','customer_name','address','locality','region','km_cust_name', 'km_address', 'km_city', 'km_state', 'km_mid','customer_status')

dfMrchMtchFinal_td.coalesce(1).write.mode('overwrite').options(header='true', quoteAll='true').csv("/km/mid/xx_mrchnt_matched_today_{}/".format(dt), emptyValue='')

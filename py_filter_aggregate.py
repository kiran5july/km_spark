



#------method:1 -assign 1 for every match & count/sum

spark.table("tb1").filter(F.col("extract_date").between("2020-04-09","2020-04-09")) \
 .select("source_system", "extract_date",
   F.when( F.col("sha_cnvr").isNull() | F.col("sha_orcl").isNull() | F.col("sha_expn").isNull(), 1).otherwise(0).alias("null_shas"),
   F.when( F.col("tran_type")=="SALE", 1).otherwise(0).alias("total_sales_tran_count")
 ).groupBy("source_system", "extract_date") \
  .agg( F.count("*").alias("total_rows")
       ,F.sum("null_shas").alias("null_shas")
       ,F.sum("total_sales_tran_count").alias("total_sales_tran_count")
  ).show()





#------Method:2 - Directly count in agg()

count_cond = lambda condn: F.sum(F.when(condn, 1).otherwise(0))

spark.table("tbl1").filter(F.col("extract_date").between("2020-04-09","2020-04-09")) \
 .groupBy("source_system", "extract_date") \
 .agg( F.count("*").alias("total_rows")
       ,count_cond(F.col("sha_cnvr").isNull() | F.col("sha_orcl").isNull() | F.col("sha_expn").isNull()).alias("null_shas")
       ,count_cond(F.col("tran_type")=="SALE").alias("total_sales_tran_count")
  ).show()
  
  

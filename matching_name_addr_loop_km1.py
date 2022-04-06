


import sys
from datetime import datetime, date
import datetime as dt
from pyspark.ml import Model, Pipeline, PipelineModel, Transformer
from pyspark.ml.feature import BucketedRandomProjectionLSH, HashingTF, MinHashLSH, NGram, RegexTokenizer
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
#from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType, IntegerType
#from pyspark.sql.functions import *


def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')


#
# Spark Session
#
print("{}: Getting spark session..".format(getDT()))
spark = SparkSession.builder \
 .enableHiveSupport() \
 .appName('KM Match - Banking MLXP') \
 .config("spark.executor.memoryOverhead", "8192") \
 .getOrCreate()
#sc = spark.sparkContext

spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")
spark.conf.set("spark.io.compression.codec", "snappy")
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.sql.shuffle.partitions", "25")

spark.sparkContext.setLogLevel("ERROR")

loop_start = 1
loop_end = 100000
increment = 100000
total_mrch = 906066

path_final_matches = "/hdfs/matches/km_matches/"
path_final_nomatch = "/hdfs/matches/km_nomatches/"

# UDF to Eliminate Non-Matching Numbers (allowing for transposed digits)
def NumberMatch(num1, num2):
  return num1==num2

spark.udf.register("NumberMatch", NumberMatch, BooleanType())
nm_udf = F.udf(NumberMatch, BooleanType())


#---ds1: 
bank_src = spark.table("kmdb.ds1").select("row_number", "company_name", F.col("address_line1").alias('ds1_addr1'), F.col("city").alias('ds1_city'), F.col("state").alias('ds1_state'), F.substring(F.col("zip"),1,5).alias('ds1_zip'))
#bank_src = spark.read.option('sep','|').csv("/km/matching_poc/ds1/") \
# .toDF('merchant_id', 'parent_company_name', 'n_store', 'n_chn', 'n_lgl', 't_mrchnt_desc', 'ss_address', 'ss_city', 'ss_state', 'ss_zipcode', 'ss_plus4_code', 'customer_status', 'customer_transaction_status') \
# .select(F.col('merchant_id').cast(IntegerType()).alias('row_number'), F.col('n_store').alias('company_name'), F.col("ss_address").alias('ds1_addr1'), F.col("ss_city").alias('ds1_city'), F.col("ss_state").alias('ds1_state'), F.substring(F.col("ss_zipcode"),1,5).alias('ds1_zip'))

#---ds2: 
df_mlxp_src = spark.table("kmdb.ds2").select("secondary_name", F.col("street_address").alias('ds2_addr1'), F.col("city").alias('ds2_city'), F.col("state_abbreviation").alias('ds2_state'), F.col("zip_code_for_street_address").alias('ds2_zip'))
#df_mlxp_src = spark.read.option('sep','|').csv("/km/matching_poc/ds2/") \
# .toDF('duns_number', 'business_name', 'secondary_name', 'street_address', 'city', 'state_abbreviation', 'zip_code_for_street_address', 'zip_4') \
# .select(F.col('business_name').alias('secondary_name'), F.col("street_address").alias('ds2_addr1'), F.col("city").alias('ds2_city'), F.col("state_abbreviation").alias('ds2_state'), F.col("zip_code_for_street_address").alias('ds2_zip') )


#----company name
df_bank_1 = bank_src.withColumn("company_name_lower", F.lower(F.col("company_name")))
df_mlxp_1 = df_mlxp_src.withColumn("company_name_lower", F.lower(F.col("secondary_name")))

df_bank_2 = df_bank_1.withColumn("company_name_std", F.regexp_replace(F.col("company_name_lower"), "[^a-z ]", ""))
df_mlxp_2 = df_mlxp_1.withColumn("company_name_std", F.regexp_replace(F.col("company_name_lower"), "[^a-z ]", ""))

#cn_len = 15
#df_bank_3 = df_bank_2.withColumn("company_name_std", F.substring(F.col("company_name_std"), 0, cn_len))
#df_mlxp_3 = df_mlxp_2.withColumn("company_name_std", F.substring(F.col("company_name_std"), 0, cn_len))


df_bank_4 = df_bank_2.drop('company_name_lower')
df_mlxp_4 = df_mlxp_2.drop('company_name_lower')

df_bank_5 = df_bank_4.filter( (F.col("company_name_std")!='') & (F.length(F.col('company_name_std'))>=3) )
df_mlxp_5 = df_mlxp_4.filter( (F.col("company_name_std")!='') & (F.length(F.col('company_name_std'))>=3) )

#---address
df_bank_6 = df_bank_5.withColumn('address_mod', F.concat(F.lower(F.col('ds1_addr1')), F.lit(' '))) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "street", "st")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "avenue", "ave")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "road", "rd")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "boulevard", "blvd")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "drive", "dr")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "lane", "ln")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "route", "rt")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "highway", "hwy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "center", "ctr")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "parkway", "pkwy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "plaza", "plz")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "freeway", "fwy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "circle", "cir")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "suite", "ste")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "court", "ct")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "expressway", "expy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "northeast", "ne")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "southeast", "se")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "northwest", "nw")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "southwest", "sw")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " north ", " n ")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " south ", " s ")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " east ", " e ")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " west ", " w "))

df_mlxp_6 = df_mlxp_5.withColumn('address_mod', F.concat(F.lower(F.col('ds2_addr1')), F.lit(' '))) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "street", "st")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "avenue", "ave")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "road", "rd")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "boulevard", "blvd")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "drive", "dr")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "lane", "ln")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "route", "rt")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "highway", "hwy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "center", "ctr")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "parkway", "pkwy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "plaza", "plz")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "freeway", "fwy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "circle", "cir")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "suite", "ste")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "court", "ct")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "expressway", "expy")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "northeast", "ne")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "southeast", "se")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "northwest", "nw")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", "southwest", "sw")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " north ", " n ")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " south ", " s ")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " east ", " e ")) \
 .withColumn("address_mod", F.regexp_replace("address_mod", " west ", " w "))

#concatenate addr1, state, city, zip
df_bank_7 = df_bank_6.withColumn("match_str", F.concat(F.col('company_name_std'), F.lit(' '), F.col("address_mod"), F.col('ds1_city'), F.lit(' '), F.col("ds1_state"), F.lit(' '), F.col('ds1_zip')) )
df_mlxp_7 = df_mlxp_6.withColumn("match_str", F.concat(F.col('company_name_std'), F.lit(' '), F.col("address_mod"), F.col('ds2_city'), F.lit(' '), F.col('ds2_state'), F.lit(' '), F.col('ds2_zip')) )

df_bank_8 = df_bank_7.withColumn("match_str", F.lower(F.col("match_str")))
df_mlxp_8 = df_mlxp_7.withColumn("match_str", F.lower(F.col("match_str")))


df_bank = df_bank_8.withColumn("match_str", F.regexp_replace(F.col("match_str"), "[^a-z0-9 ]", ""))
df_mlxp = df_mlxp_8.withColumn("match_str", F.regexp_replace(F.col("match_str"), "[^a-z0-9 ]", ""))

df_bank = df_bank.filter( (F.col("match_str")!='') & (F.length(F.col('match_str'))>=3) )
df_mlxp = df_mlxp.filter( (F.col("match_str")!='') & (F.length(F.col('match_str'))>=3) )

df_bank.cache()
df_mlxp.cache()

print("BANK count: " + str(df_bank.count()) )
print("MLXP count: " + str(df_mlxp.count()) )

while(loop_end <= total_mrch):
    print(getDT()+": Batch: " + str(loop_start) + " -> " + str(loop_end))
    df_bank_batch = df_bank.filter(F.col('row_number').between(loop_start, loop_end))
    #df_bank_batch.cache()

    pipeline = Pipeline(stages=[
        RegexTokenizer(pattern="", inputCol="match_str", outputCol="tokens", minTokenLength=1 ),
        NGram(n=3, inputCol="tokens", outputCol="ngrams"),
        HashingTF(inputCol="ngrams", outputCol="vectors"),
        MinHashLSH(inputCol="vectors", outputCol="lsh")
    ])

    model = pipeline.fit(df_bank_batch.select("match_str"))

    hashed_bank = model.transform(df_bank_batch.select("match_str"))
    hashed_mlxp = model.transform(df_mlxp.select("match_str"))

    df_matched = model.stages[-1].approxSimilarityJoin(hashed_bank, hashed_mlxp, 0.4, "confidence")
    df_results = df_matched.select(F.col("datasetA.match_str").alias("match_str_bank"), F.col("datasetB.match_str").alias("match_str_mlxp"), F.col("confidence"))

    #df_results.cache()

    df_final_matches = df_bank_batch.join(df_results, df_bank_batch.match_str==df_results.match_str_bank).select("row_number", "company_name", "match_str_bank", "match_str_mlxp", "confidence")

    df_final_matches.coalesce(1).write.option("sep","|").mode('append').format("csv").save(path_final_matches)


    #Dump Non-Matches into Separate Table
    #df_final_nomatch = df_bank_batch.join(df_results, df_bank_batch.match_str==df_results.match_str_bank, how='leftanti') \
    # .select('row_number', 'company_name') \

    #df_final_nomatch.coalesce(1).write.option("sep","|").mode('append').format("csv").save(path_final_nomatch)

    #df_results.unpersist()
    #df_bank_batch.unpersist()

    loop_start = loop_end + 1
    loop_end = loop_end + increment


print("{}:----- COMPLETED ----".format(getDT()))



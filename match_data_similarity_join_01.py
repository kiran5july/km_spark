import sys
from datetime import datetime, date
import datetime as dt
from pyspark.ml import Model, Pipeline, PipelineModel, Transformer
from pyspark.ml.feature import BucketedRandomProjectionLSH, HashingTF, MinHashLSH, NGram, RegexTokenizer
from pyspark.ml.linalg import Vectors
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
from pyspark.sql.functions import *

def getDT():
 return datetime.now().strftime( '%Y-%m-%d %H:%M:%S')

output_path_matches = ""
output_path_nomatches = ""

#
# Spark Session
#
print("{}: Getting spark session..".format(getDT()))
spark = SparkSession.builder.enableHiveSupport().appName('KM Match datasets') \
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


#
# UDF to Eliminate Zero Arrays for Hasher
#

def NonZeroVector(v):
	return v.numNonzeros() > 0

spark.udf.register("NonZeroVector", NonZeroVector, BooleanType())
nzf_udf = udf(NonZeroVector, BooleanType())

# Transformer Class to Eliminate Zero Arrays for Hasher

class ZeroDropper(Transformer):
	def __init__(self):
		super(ZeroDropper, self).__init__()
		
	def _transform(self, df):
		df = df.filter(nzf_udf("vectors"))
		return df

# UDF to Eliminate Non-Matching Numbers (allowing for transposed digits)
def NumberMatch(num1, num2):
	return num1==num2

#return len(set(num1).difference(set(num2))) <= 1

spark.udf.register("NumberMatch", NumberMatch, BooleanType())
nm_udf = udf(NumberMatch, BooleanType())


#
# Queries
#
# *** These table names and queries will change for production. ***


df_ds1_src = spark.createDataFrame([('111', '123 Main St', '', 'Cincinnati', 'OH', '11111')]) \
 .toDF('id', 'ds1_address_line1', 'ds1_address_line2', 'ds1_city', 'ds1_state', 'ds1_zip')

df_ds2_src = spark.createDataFrame([('123 Main St', '', 'Cincinnati', 'OH','11111','1'), ('123 Chicago St', '', 'Cincinnati', 'OH','22222','2'), ('123 Main St', '', 'Chicago', 'IL','34343','3'), ('123 Mains St', '', 'Cincinnati', 'OH','22222','2')]) \
 .toDF('ds2_address_line1', 'ds2_address_line2', 'ds2_city', 'ds2_state', 'ds2_zip', 'index')


#concatenate state, name to address
df_ds1_1 = df_ds1_src.withColumn("address_std", F.concat(F.col('ds1_zip'), F.lit(' '), F.col("ds1_state"), F.lit(" "), F.col("ds1_address_line1")))
df_ds2_1 = df_ds2_src.withColumn("address_std", F.concat(F.col('ds2_zip'), F.lit(' '), F.col("ds2_state"), F.lit(" "), F.col("ds2_address_line1")))

# Lower Case Transformation
df_ds1_2 = df_ds1_1.withColumn("address_std", F.lower(F.col("address_std")))
df_ds2_2 = df_ds2_1.withColumn("address_std", F.lower(F.col("address_std")))


# Keep only the alphanumeric characters for address, company name
df_ds1_3 = df_ds1_2.withColumn("address_std", F.regexp_replace(F.col("address_std"), "[^a-z0-9 ]", ""))
df_ds2_3 = df_ds2_2.withColumn("address_std", F.regexp_replace(F.col("address_std"), "[^a-z0-9 ]", ""))



# Drop NA Addresses
df_ds1_4 = df_ds1_3.na.drop(subset=["address_std"])
df_ds2_4 = df_ds2_3.na.drop(subset=["address_std"])


# Stem company name and address

addr_len = 25
df_ds1_5 = df_ds1_4.withColumn("address_std", F.substring(F.col("address_std"), 0, addr_len))
df_ds2_5 = df_ds2_4.withColumn("address_std", F.substring(F.col("address_std"), 0, addr_len))



# Street Abbreviations

df_ds1_6 = df_ds1_5 \
 .withColumn("address_std", F.regexp_replace("address_std", "street", "st")) \
 .withColumn("address_std", F.regexp_replace("address_std", "avenue", "ave")) \
 .withColumn("address_std", F.regexp_replace("address_std", "road", "rd")) \
 .withColumn("address_std", F.regexp_replace("address_std", "boulevard", "blvd")) \
 .withColumn("address_std", F.regexp_replace("address_std", "drive", "dr")) \
 .withColumn("address_std", F.regexp_replace("address_std", "lane", "ln")) \
 .withColumn("address_std", F.regexp_replace("address_std", "route", "rt")) \
 .withColumn("address_std", F.regexp_replace("address_std", "highway", "hwy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "center", "ctr")) \
 .withColumn("address_std", F.regexp_replace("address_std", "parkway", "pkwy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "plaza", "plz")) \
 .withColumn("address_std", F.regexp_replace("address_std", "freeway", "fwy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "circle", "cir")) \
 .withColumn("address_std", F.regexp_replace("address_std", "suite", "ste")) \
 .withColumn("address_std", F.regexp_replace("address_std", "court", "ct")) \
 .withColumn("address_std", F.regexp_replace("address_std", "expressway", "expy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "northeast", "ne")) \
 .withColumn("address_std", F.regexp_replace("address_std", "southeast", "se")) \
 .withColumn("address_std", F.regexp_replace("address_std", "northwest", "nw")) \
 .withColumn("address_std", F.regexp_replace("address_std", "southwest", "sw")) \
 .withColumn("address_std", F.regexp_replace("address_std", " north ", " n ")) \
 .withColumn("address_std", F.regexp_replace("address_std", " south ", " s ")) \
 .withColumn("address_std", F.regexp_replace("address_std", " east ", " e ")) \
 .withColumn("address_std", F.regexp_replace("address_std", " west ", " w "))

df_ds2_6 = df_ds2_5 \
 .withColumn("address_std", F.regexp_replace("address_std", "street", "st")) \
 .withColumn("address_std", F.regexp_replace("address_std", "avenue", "ave")) \
 .withColumn("address_std", F.regexp_replace("address_std", "road", "rd")) \
 .withColumn("address_std", F.regexp_replace("address_std", "boulevard", "blvd")) \
 .withColumn("address_std", F.regexp_replace("address_std", "drive", "dr")) \
 .withColumn("address_std", F.regexp_replace("address_std", "lane", "ln")) \
 .withColumn("address_std", F.regexp_replace("address_std", "route", "rt")) \
 .withColumn("address_std", F.regexp_replace("address_std", "highway", "hwy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "center", "ctr")) \
 .withColumn("address_std", F.regexp_replace("address_std", "parkway", "pkwy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "plaza", "plz")) \
 .withColumn("address_std", F.regexp_replace("address_std", "freeway", "fwy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "circle", "cir")) \
 .withColumn("address_std", F.regexp_replace("address_std", "suite", "ste")) \
 .withColumn("address_std", F.regexp_replace("address_std", "court", "ct")) \
 .withColumn("address_std", F.regexp_replace("address_std", "expressway", "expy")) \
 .withColumn("address_std", F.regexp_replace("address_std", "northeast", "ne")) \
 .withColumn("address_std", F.regexp_replace("address_std", "southeast", "se")) \
 .withColumn("address_std", F.regexp_replace("address_std", "northwest", "nw")) \
 .withColumn("address_std", F.regexp_replace("address_std", "southwest", "sw")) \
 .withColumn("address_std", F.regexp_replace("address_std", " north ", " n ")) \
 .withColumn("address_std", F.regexp_replace("address_std", " south ", " s ")) \
 .withColumn("address_std", F.regexp_replace("address_std", " east ", " e ")) \
 .withColumn("address_std", F.regexp_replace("address_std", " west ", " w "))


#df_ds1_6.registerTempTable("xxx")
#df_ds1_6.cache()
#df_ds2_6.cache()


#def _transform(self, df: DataFrame) -> DataFrame:

zd = ZeroDropper()

# Model

pipeline = Pipeline(stages=[
        RegexTokenizer(
            pattern="", inputCol="address_std", outputCol="tokens", minTokenLength=1
            ),
        NGram(n=3, inputCol="tokens", outputCol="ngrams"),
        HashingTF(inputCol="ngrams", outputCol="vectors"),
        zd,
        MinHashLSH(inputCol="vectors", outputCol="lsh")
    ])

model = pipeline.fit(df_ds1_6.select("address_std"))

hashed_ds1 = model.transform(df_ds1_6.select("address_std"))
hashed_ds2 = model.transform(df_ds2_6.select("address_std"))

df_matched = model.stages[-1].approxSimilarityJoin(hashed_ds1, hashed_ds2, 1.0, "confidence")
df_results1 = df_matched.select(F.col("datasetA.address_std").alias("address_ds1"), F.col("datasetB.address_std").alias("address_ds2"), F.round(F.col('confidence'), 3).alias('confidence') )

# Remove Non-Matching States from DataFrame
df_results3 = df_results1 \
   .withColumn("state_ds1", F.regexp_extract(F.col("address_ds1"), "^(\d+ \w+)", 1)) \
   .withColumn("state_ds2", F.regexp_extract(F.col("address_ds2"), "^(\d+ \w+)", 1)) \
   .filter(F.col("state_ds1")==F.col("state_ds2")).drop('state_ds1', 'state_ds2')


# Remove Non-Matching Numbers from DataFrame
#df_results3 = df_st_filter.withColumn("number_ds1", F.regexp_extract(F.col("address_ds1"), "^\D*(\d+)", 1)) \
#  .withColumn("number_ds2", F.regexp_extract(F.col("address_ds2"), "^\D*(\d+)", 1)) \
#  .withColumn("match", nm_udf(F.col("number_ds1"), F.col("number_ds2"))) \
#  .filter(F.col("match") == True) \
#  .drop('number_ds1', 'number_ds2', 'match')


 
#df_results3.cache()
##df_ds1_6.cache()

df_match = df_results3.join(df_ds1_6, df_results3.address_ds1==df_ds1_6.address_std, how='left')


df_match_final = df_match \
        .join(df_ds2_6, df_match.address_ds2==df_ds2_6.address_std, how='left')

#       .dropDuplicates(subset=["address_ds1"])

df_match_out = df_match_final.select('id', \
                                F.col('ds1_address_line1'), \
                                F.col('ds1_address_line2'), \
                                F.col('ds1_city'), \
                                F.col('ds1_state'), \
                                F.col('ds1_zip'), \
                                F.col('ds2_address_line1'), \
                                F.col('ds2_address_line2'), \
                                F.col('ds2_city'), \
                                F.col('ds2_state'), \
                                F.col('ds2_zip'), \
                                F.col('index'), F.col("confidence"))

df_match_out.show(5, False)
#df_match_out.repartition(1).write.mode('overwrite').options(sep='|', header='true', quoteAll='false').csv(output_path_matches, emptyValue='')

#Dump Non-Matches into Separate Table

df_nomatch_final = df_ds1_6.join(df_results3, df_ds1_6.address_std==df_results3.address_ds1, how='leftanti')

df_nomatch_out = df_nomatch_final.select('id', \
                                F.col('ds1_address_line1'), \
                                F.col('ds1_address_line2'), \
                                F.col('ds1_state'), \
                                F.col('ds1_city'), \
                                F.col('ds1_zip'))
    
df_nomatch_out.show(5, False)
#df_nomatch_out.repartition(1).write.mode('overwrite').options(sep='|', header='true', quoteAll='false').csv(output_path_nomatches, emptyValue='')

#df_results3.unpersist()


print("{}:----- COMPLETED ----".format(getDT()))


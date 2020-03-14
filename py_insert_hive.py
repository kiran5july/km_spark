


from pyspark.sql.types import IntegerType, StringType
import pyspark.sql.functions as F
from datetime import datetime, date

sDBName="kmdb"

#Ext table
CREATE EXTERNAL TABLE kmdb.test_ext (id STRING) LOCATION '/kmdb/common/test_ext';
spark.createDataFrame([("111")], StringType()).write.insertInto("{}.test_ext".format(sDBName), overwrite=True)


#partitioned table
spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")

CREATE EXTERNAL TABLE kmdb.test_ext_prtn (id STRING)
 PARTITIONED BY (extract_date string)
 LOCATION '/kmdb/common/test_ext_prtn';

#df.write.mode("overwrite") :-> is not working
spark.createDataFrame([("2020030202")], StringType()) \
 .withColumn("extract_date", F.lit('2020-03-02')) \
 .write.insertInto("{}.test_ext_prtn".format(sDBName), overwrite=True)


spark.createDataFrame([("2020030301")], StringType()) \
 .withColumn("extract_date", F.lit(datetime.today().strftime('%Y-%m-%d'))) \
 .write.insertInto("{}.test_ext_prtn".format(sDBName), overwrite=True)

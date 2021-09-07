

from pyspark.sql import SparkSession
spark=SparkSession.builder.master("yarn").appName('KM Pyspark Job').enableHiveSupport().getOrCreate()

sc=spark.sparkContext

sc.getConf().getAll()
for item in sorted(sc.getConf().getAll()): print(item)


#Read into List
listConf=sc.getConf().getAll()

for i in listConf: print("{} = {}".format(i[0],i[1]) )
for k,v in listConf: print "{} = {}".format(k, v)

[p for p in listConf if 'compress' in p[0]]


#Read List into Dict
dictConf={i[0]: i[1] for i in listConf}

for key in dictConf.keys(): print("{} = {}".format(key, dictConf[key]))
  

  
#Read into Dataframe
df_params = sc.parallelize(sc.getConf().getAll()).toDF(['Param', 'Val'])

df_params = spark.createDataFrame(sc.getConf().getAll(), ["param","val"])

import pyspark.sql.functions as O
df_params.filter(O.col('param').contains('compress')).show(10,False)





from pyspark.sql import SparkSession
spark=SparkSession.builder.master("yarn").appName('KM Pyspark Job').enableHiveSupport().getOrCreate()
sc=spark.sparkContext



sc.getConf().getAll()
for item in sorted(sc.getConf().getAll()): print(item)


#Read into Dictionary
listConf=sc.getConf().getAll()
dictConf={i[0]: i[1] for i in listConf}
for key in dictConf.keys(): print("{} = {}".format(key, dictConf[key]))


#Read into Dataframe
sc.parallelize(sc.getConf().getAll()).toDF(['Param', 'Val']).show(50, False)

spark.createDataFrame(sc.getConf().getAll(), ["param","val"]).show(50,False)




//-------SparkSession------------
val spark = SparkSession.builder
  .appName("KM App")
  .master("yarn")
  .enableHiveSupport()
  .getOrCreate()


val conf = new SparkConf()
   .setMaster("local") //yarn
   .setAppName("kmiry_Timed_Job")

val spark = SparkSession.builder
   .config(conf)
   //.enableHiveSupport()
   .getOrCreate

//-----------------------------
spark.conf.getAll.foreach(println)
spark.sql("SET -v").show(200, truncate=false)
spark.conf.getAll.toList



//to Map
val configMap:Map[String, String] = spark.conf.getAll
OR: var configMap2=spark.sparkContext.getConf.getAll

for ((k,v) <- configMap) println(s"$k -> $v")



//to DF
spark.conf.getAll.toList.toDF("param", "val").show(false)
(RDD: sc.parallelize(spark.conf.getAll.toList).

//Get a particular config property
spark.conf.get("spark.sql.shuffle.partitions")

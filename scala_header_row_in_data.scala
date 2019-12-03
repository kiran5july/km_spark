


------- Remove for any column headers in data
val dfData = spark.sql("select * from mkdb.exp_raw")
val fileHeader = dfData.head.getString(0)
val dfRealData = dfData.filter(col(dfData.columns(0)) =!= fileHeader)

---RDD
val file = spark.textFile("/file_path")
val header = file.first
val data = file.map(row => row != header)

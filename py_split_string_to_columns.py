
---------split string
df1 = spark.createDataFrame([("1", "abc xyz"), ("2","def g hij"), ("3", "abcd")]).toDF("id", "full_name")

df1.withColumn('name_strings', F.split(F.col('full_name'),' ')) \
 .withColumn('first_name', F.col('name_strings')[0] ) \
 .withColumn('last_name', F.element_at(F.col('name_strings'),-1) ).show(5)

# .withColumn('last_name', F.when(F.size(F.col('name_strings'))>1, F.element_at(F.col('name_strings'),-1) ).otherwise(''))


--FULL: populate if null/blank
df1 = spark.createDataFrame([("1", "abc xyz", "",""), ("2","abcd","",""), ("3", "", "",""), ("4", None, "",None), ("5", "def ghi jkl", "","")]).toDF("id", "full_name", "last_name","first_name")
df1.withColumn('name_strings', F.split(F.col('full_name'),' ')).withColumn('string_valid', F.when( F.size(F.col('name_strings'))>1, 'correct format').otherwise('incorrect') ) \
 .withColumn('first_name2', F.when((F.col('first_name').isNull()) | (F.col('first_name')==''), F.when( F.size(F.col('name_strings'))>1, F.col('name_strings')[0]).otherwise('')).otherwise(F.col('first_name')) ) \
 .withColumn('last_name2', F.when((F.col('last_name').isNull()) | (F.col('last_name')==''), F.when( F.size(F.col('name_strings'))>1, F.element_at(F.col('name_strings'),-1)).otherwise('')).otherwise(F.col('last_name')) ).show(5)


//---Split a column & populate into dynamic columns

df1 = spark.createDataFrame([("a","a2,a1"), ("b", "b1,b2,b3")]).toDF("A","B")
df2 = df1.withColumn("b_split", F.sort_array(F.split(F.col("B"), ",")))

//---Split into static columns
df2.withColumn("col_0", F.col("b_split").getItem(0) ) \
 .withColumn("col_1", F.col("b_split").getItem(1) ) \
 .withColumn("col_2", F.col("b_split").getItem(2) ) \
 .show()




//---dynamically
from functools import reduce
import re
def split_to_columns(df, cl):
 cint = int(re.sub("[^0-9]", "", cl))
 return df.withColumn(cl, F.col("b_split").getItem(cint))

new_columns = ['B0','B1','B2','B3','B4','B5']
df3 = reduce(split_to_columns, new_columns, df2)
df3.show(5)
+---+--------+------------+---+---+----+----+----+----+
|  A|       B|     b_split| B0| B1|  B2|  B3|  B4|  B5|
+---+--------+------------+---+---+----+----+----+----+
|  a|   a2,a1|    [a1, a2]| a1| a2|null|null|null|null|
|  b|b1,b2,b3|[b1, b2, b3]| b1| b2|  b3|null|null|null|
+---+--------+------------+---+---+----+----+----+----+



--TRY
df2.select( lambda x: F.col("B_arr").getItem(i).as(s"col_$i")): _*).show()


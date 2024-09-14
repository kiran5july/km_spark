//--------------read json & add/update an element
//----add/update event.location.id & populate with outside id

Sample data:
/home/km/km/km_practice/data/event_locations.json
{"id":"1","event":{"location":{"id":"xxx", "country":""},"location3":{"country":"US"}},"date":1575912010}
{"id":"2","event":{"location":{"country":"CA"},"location3":{"country":""}},"date":1575912070}
{"id":"3","event":{"location":{"country":"US"},"location3":{"country":""}},"date":1573320128}
{"id":"4","event":{"location":{"country":""},"location3":{"country":"ME"}},"date":1575912121}
{"id":"5","event":{"location":{"country":"CA"},"location3":{"country":""}},"date":1575912249}
{"id":"6","event":{"location":{"country":"CA"},"location3":{"country":""}},"date":1573320610}
{"id":"7","event":{"location":{"country":"ME"},"location3":{"country":""}},"date":1575912608}
{"id":"8","event":{"location":{"country":""},"location3":{"country":"IN"}},"date":1575912633}


//Output:
{"id":"1","event":{"location":{"id":"1", "country":""},"location3":{"country":"US"}},"date":1575912010}
{"id":"2","event":{"location":{"id":"2", "country":"CA"},"location3":{"country":""}},"date":1575912070}

//Method#1: read json & update using schema


//Method#2: add/update particular element (add id inside location)
val df_json = spark.read.json("file:////home/km/km/km_practice/data/event_locations.json")

val df_json2 = df_json.withColumn("event_str", to_json($"event"))
import org.apache.spark.sql.functions.{from_json,to_json}
import org.apache.spark.sql.types.{StructType,MapType,StringType}
val df_json2_x = df_json2.withColumn("event2", from_json($"event_str", MapType(StringType, StringType) )).select("id","event_str","event2")
df_json2_x.show(5,false)


//df_json2_x.withColumn("location_str", $"event2.location").show(5,false)

import org.apache.spark.sql.catalyst.ScalaReflection
case class Location(id: Option[String], country: Option[String])
val schema_loc = ScalaReflection.schemaFor[Location].dataType.asInstanceOf[StructType]

val updateMrch = udf((m : Map[String, String], loc_new: String) =>
     m.map{ case (key, value) => (key, if (key == "location") loc_new else value) }
)

val df_json2_x2 = df_json2_x.withColumn("location", to_json(from_json($"event2.location", schema_loc).withField("id", $"id"))).
 withColumn("event2", updateMrch($"event2", $"location") ).
 withColumn("event_str2", to_json($"event2")).select("id","event_str", "event_str2").
 withColumn("event_str2", regexp_replace($"event_str2", lit("\\\\"), lit("") ))  //.show(5,false)


df_json2_x2.select("id","event_str2").withColumnRenamed("event_str2","event").show(5,false)
+---+---------------------------------------------------------------------+
|id |event                                                                |
+---+---------------------------------------------------------------------+
|1  |{"location":"{"id":"1","country":""}","location3":"{"country":"US"}"}|
|2  |{"location":"{"id":"2","country":"CA"}","location3":"{"country":""}"}|
|3  |{"location":"{"id":"3","country":"US"}","location3":"{"country":""}"}|
|4  |{"location":"{"id":"4","country":""}","location3":"{"country":"ME"}"}|
|5  |{"location":"{"id":"5","country":"CA"}","location3":"{"country":""}"}|
+---+---------------------------------------------------------------------+

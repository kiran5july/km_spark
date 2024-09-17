import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

val df_json = spark.read.json("file:////Users/km/Desktop/km/km_practice/data/event_locations.json")


//val data_str = """{"id":"1","event":{"location":{"id":"xx","state":"WA","country":""}}}"""
//data_str.toDF("value").show(5,false)
//val df = spark.createDataFrame(data_str.toSeq)

df_json.printSchema()

val df_json_str = df_json.withColumn("event_str", to_json($"event")).drop("date")


//-----------read json as string & convert to json doc using full schema
val df_json_txt = spark.read.text("file:////Users/km/Desktop/km/km_practice/data/event_locations.json")

case class Location(id: Option[String], state: Option[String], country: Option[String])
case class Event(location: Option[Location])
case class Events(id: Option[String], event: Option[Event], date: Option[String])

val schema_events = ScalaReflection.schemaFor[Events].dataType.asInstanceOf[StructType]

//using Encoders --WORKS
//import org.apache.spark.sql.Encoders
//val schema2 = Encoders.product[Event].schema

val df_json_hier = df_json_txt.withColumn("event", from_json($"value", schema_events))


//-------------json replace as string
val df = spark.createDataFrame(Seq(
  (1, """{"a":"a1"}""", """{"a":""", """{"A":"""),
  (2, """{"b":"b2"}""", """{"b":""", """{"B":"""),
  (3, """{"a":"c1"}""", """{"a":""", """{"A":"""))).toDF("Id", "json_str", "to_replace", "replace_with")

df.withColumn("new_json_str", regexp_replace($"json_str", "\"a\"", "\"A\"")).show(5,false)

val df2 = df.withColumn("to_replace_pattern", regexp_replace($"to_replace", "\"", "\\\\\"")).
  withColumn("to_replace_pattern", regexp_replace($"to_replace_pattern", "\\{", "\\\\\\{")).
  withColumn("replace_with_pattern", regexp_replace($"replace_with", "\"", "\\\\\""))

//df2.withColumn("to_replace", regexp_replace($"to_replace_pattern", "\\{", "\\\\\\{")).show(5,false)

df2.withColumn("new_json_str", regexp_replace($"json_str", $"to_replace_pattern", $"replace_with_pattern")).show(5,false)

//-------------------


//-----------extract json doc string from json string
df_json_str.withColumn("event", get_json_object($"event_str", "$")).show(5,false)
//returns string ONLY

val df_json3 = df_json_str.withColumn("location", get_json_object($"event_str", "$.location"))

val schema_loc = ScalaReflection.schemaFor[Location].dataType.asInstanceOf[StructType]

val df_json4 = df_json3.withColumn("location2", to_json(from_json($"location", schema_loc).withField("id", $"id")))

val df_json5 = df_json4.withColumn("location_pattern", regexp_replace($"location", "\"", "\\\\\"")).
  withColumn("location_pattern", regexp_replace($"location_pattern", "\\{", "\\\\\\{")).
  withColumn("location2_pattern", regexp_replace($"location2", "\"", "\\\\\""))

df_json5.withColumn("event_str2", when($"location".isNull, $"event_str").otherwise(regexp_replace($"event_str", $"location_pattern", $"location2_pattern")) ).select("event_str", "event_str2").show(5,false)

df_json5.withColumn("event_str2", when($"location".isNull, $"event_str").otherwise( lit("xxx")) ).show(5,false)


//-----------json doc in string column

val pmt_dtl_data = scala.io.Source.fromFile("/Users/km/Downloads/cb_matched_pmt_dtl_orig_event_01.json").mkString

val df_json_str = Seq(("111111", pmt_dtl_data)).toDF("MERCHANT_ID", "MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT")

import org.apache.spark.sql.functions.{from_json, to_json}
import org.apache.spark.sql.types.{StructType,MapType,StringType}
import org.apache.spark.sql.catalyst.ScalaReflection
case class MerchantDetail(
                           administrationCode: String,
                           merchantCode: String,
                           merchantCountryCode: Option[String],
                           shopCountryCode: Option[String],
                           subscriptionType: String,
                           merchantId: Option[String]
                         )
val schema_mrch = ScalaReflection.schemaFor[MerchantDetail].dataType.asInstanceOf[StructType]

//--update as string
//get merchant doc
val df_json_str_mrch = df_json_str.withColumn("mrch_str", get_json_object($"MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT", "$.originatingEvent.merchant"))
//returns string ONLY

val df_json_str_mrch2 = df_json_str_mrch.withColumn("mrch_str2", to_json(from_json($"mrch_str", schema_mrch).withField("merchantId", $"merchant_id")))

val df_json_str_mrch3 = df_json_str_mrch2.withColumn("mrch_pattern", regexp_replace($"mrch_str", "\"", "\\\\\"")).
  withColumn("mrch_pattern", regexp_replace($"mrch_pattern", "\\{", "\\\\\\{")).
  withColumn("mrch_pattern_to", regexp_replace($"mrch_str2", "\"", "\\\\\""))

df_json_str_mrch3.select("mrch_pattern","mrch_pattern_to").show(5,false)

df_json_str_mrch3.withColumn("MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT2", when($"mrch_str2".isNull, $"MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT").otherwise(regexp_replace($"MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT", $"mrch_pattern", $"mrch_pattern_to")) ).
  select("MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT", "MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT2").show(5,false)



//---update as Map elements (originatingEvent.merchant.merchantId) --YET TO FIX---
val df_json_str_x = df_json_str.withColumn("mtch_pmt_evnt_map", from_json($"MATCHED_PAYMENT_DETAILS_ORIGINATING_EVENT", MapType(StringType, StringType) )).select("merchant_id","mtch_pmt_evnt_map")
df_json_str_x.show(5,false)

//df_json_str_x.withColumn("location_str", $"event_map.location").show(5,false)

val updateMrch = udf((m : Map[String, String], loc_new: String) =>
     m.map{ case (key, value) => (key, if (key == "merchant") loc_new else value) }
)

val df_json_str_x2 = df_json_str_x.withColumn("mtch_pmt_evnt_map_mrch", to_json(from_json($"mtch_pmt_evnt_map.merchant", schema_mrch).withField("merchantid", $"merchant_id"))).
 withColumn("mtch_pmt_evnt_map", updateMrch($"mtch_pmt_evnt_map", $"mtch_pmt_evnt_map_mrch") ).
 withColumn("mtch_pmt_evnt_str2", to_json($"mtch_pmt_evnt_map")).select("merchant_id","mtch_pmt_evnt_str2").
 withColumn("mtch_pmt_evnt_str2", regexp_replace($"mtch_pmt_evnt_str2", lit("\\\\"), lit("") ))  //.show(5,false)



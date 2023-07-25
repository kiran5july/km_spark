
import pyspark.sql.functions as F
from pyspark.sql.types import StringType
data = spark.createDataFrame([('ILLINOIS'), ('North Carolina'), ('XX'), ('NEWYORK')], StringType()).toDF('state')


state_abbr = {'IOWA': 'IA', 'KANSAS': 'KS', 'FLORIDA': 'FL', 'VIRGINIA': 'VA', 'NORTH CAROLINA': 'NC', 'SOUTH DAKOTA': 'SD', 'ALABAMA': 'AL', 'IDAHO': 'ID',
  'DELAWARE': 'DE', 'ALASKA': 'AK', 'CONNECTICUT': 'CT', 'TENNESSEE': 'TN', 'PUERTO RICO': 'PR', 'NEW MEXICO': 'NM', 'MISSISSIPPI': 'MS',
  'COLORADO': 'CO', 'NEW JERSEY': 'NJ', 'UTAH': 'UT', 'MINNESOTA': 'MN', 'VIRGIN ISLANDS': 'VI', 'NEVADA': 'NV',
  'ARIZONA': 'AZ', 'WISCONSIN': 'WI', 'NORTH DAKOTA': 'ND', 'PENNSYLVANIA': 'PA', 'OKLAHOMA': 'OK', 'KENTUCKY': 'KY',
  'RHODE ISLAND': 'RI', 'NEW HAMPSHIRE': 'NH', 'MISSOURI': 'MO', 'MAINE': 'ME', 'VERMONT': 'VT', 'GEORGIA': 'GA',
  'GUAM': 'GU', 'AMERICAN SAMOA': 'AS', 'NEW YORK': 'NY', 'CALIFORNIA': 'CA', 'HAWAII': 'HI', 'ILLINOIS': 'IL', 'NEBRASKA': 'NE',
  'MASSACHUSETTS': 'MA', 'OHIO': 'OH', 'MARYLAND': 'MD', 'MICHIGAN': 'MI', 'WYOMING': 'WY', 'WASHINGTON': 'WA', 'OREGON': 'OR', 'SOUTH CAROLINA': 'SC',
  'INDIANA': 'IN', 'LOUISIANA': 'LA', 'NORTHERN MARIANA ISLANDS': 'MP', 'DISTRICT OF COLUMBIA': 'DC', 'MONTANA': 'MT', 'ARKANSAS': 'AR', 'WEST VIRGINIA': 'WV', 'TEXAS': 'TX'}

b_lst_format_state = spark.sparkContext.broadcast(state_abbr)
addr_format_state = b_lst_format_state.value


#------directly from dict --MAY NOT BE POSSIBLE AS DATA IS NOT RETURNED UNTIL ACTION ---
data.withColumn('state_formatted', F.lit(addr_format_state.get(F.upper(F.col("state")).cast("string"), 'INVALID')) ).show(5)

#------using create_map & chain
from itertools import chain
mapping_expr = F.create_map([F.lit(x) for x in chain(*addr_format_state.items())])

data.withColumn('state_formatted', mapping_expr.getItem(F.upper(F.col('state'))) ).show(5)
+--------------+---------------+
|         state|state_formatted|
+--------------+---------------+
|      ILLINOIS|             IL|
|North Carolina|             NC|
|            XX|           null|
|       NEWYORK|           null|
+--------------+---------------+


#------using udf
#one-liner udf
formatSateUDF = F.udf(lambda x: addr_format_state.get(x, 'INVALID') )

#expanded udf
def formatState(s):
 #addr_format_state = b_lst_format_state.value
 s = addr_format_state.get(s, 'INVALID')
 return s

formatSateUDF = F.udf(lambda x: formatState(x))

data.withColumn('state_formatted', formatSateUDF(F.upper(F.col("state"))) ).show(5)
####data.withColumn('state_formatted', F.when(F.length(F.col('state'))==2, F.upper(F.col("state"))).when(F.length(F.col('state'))>2, formatSateUDF(F.upper(F.col("state"))) ).otherwise(F.lit('')) ).show(5)

+--------------+---------------+
|         state|state_formatted|
+--------------+---------------+
|      ILLINOIS|             IL|
|North Carolina|             NC|
|            XX|        INVALID|
|       NEWYORK|        INVALID|
+--------------+---------------+

#----using udf with passing dict into udf
from pyspark.sql.types import StringType
def getStateAbbr(state_lookup):
  return F.udf(lambda col: state_lookup.get(col.upper(),'INVALID'), StringType())

--OR--
def getStateAbbr2(mapping_list):
    def translate_(col):
        return mapping_list.get(col.upper(), 'INVALID')
    return F.udf(translate_, StringType())

data.withColumn("state_formatted", getStateAbbr(addr_format_state)('state') ).show(5)

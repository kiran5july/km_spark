



import pyspark.sql.functions as F
from pyspark.sql.types import StringType
data = spark.createDataFrame([('ILLINOIS'), ('North Carolina'), ('XX')], StringType()).toDF('state')


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

#using udf & regex
import re
def formatState(s):
 #addr_format_state = b_lst_format_state.value
 #for x in addr_format_state: s = re.sub(x, addr_format_state[x], s)
 s = addr_format_state.get(s, 'INVALID')
 return s

formatSateUDF = F.udf(lambda x: formatState(x))

data.withColumn('state_formatted', F.when(F.length(F.col('state'))==2, F.upper(F.col("state"))).when(F.length(F.col('state'))>2, formatSateUDF(F.upper(F.col("state"))) ).otherwise(F.lit('')) ).show(5)
+--------------+---------------+
|         state|state_formatted|
+--------------+---------------+
|      ILLINOIS|             IL|
|North Carolina|             NC|
|            XX|             XX|
+--------------+---------------+

#using itertools
from itertools import chain
lookup_map = F.create_map(*[F.lit(x) for x in chain(*state_abbr.items())])
data.withColumn('state_formatted', F.when(F.length(F.col('state'))==2, F.upper(F.col("state"))).when(F.length(F.col('state'))>2, lookup_map[F.upper(F.col("state"))] ).otherwise(F.lit('')) ).show(5)
+--------------+---------------+
|         state|state_formatted|
+--------------+---------------+
|      ILLINOIS|             IL|
|North Carolina|             NC|
|            XX|             XX|
+--------------+---------------+


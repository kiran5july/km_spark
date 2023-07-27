#------show the percentage of price change from previous time
#sample data
part_id|unit_price|part_update_timestamp
111|5.99|2022-12-01 07:35:56
111|6.99|2022-12-05 08:12:34
222|25.99|2022-12-02 08:02:45
222|29.89|2022-12-12 08:58:31
333|9.99|2022-12-15 04:38:44

Output:


df_parts_dim = spark.read.options(sep='|', header=True).csv('/km_practice/data/parts_prices.csv')

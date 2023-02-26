


access_key = "xx"
secret_access_key = "yy"
bucket_name = "abc"

spark._jsc.hadoopConfiguration().set("fs.s3n.awsAccessKeyId", access_key)
spark._jsc.hadoopConfiguration().set("fs.s3n.awsSecretAccessKey", secret_access_key)


spark.read.option("header", True).csv(f's3://{bucket_name}/2023-01-21/controlFile*').show(5)


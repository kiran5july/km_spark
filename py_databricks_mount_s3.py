
access_key = "xx"
secret_access_key = "yy"

encoded_secret_key = secret_access_key.replace("/", "%2F")
aws_bucket_name = "abc"
mount_name = "/mnt/km_s3_mount"


#---mount it to Databricks DBFS
#dbutils.fs.mount(f"s3a://{bucket_name}", mount_name)
dbutils.fs.mount(f"s3a://{access_key}:{encoded_secret_key}@{aws_bucket_name}", mount_name)

display(dbutils.fs.ls(mount_name))

#---unmount
#dbutils.fs.unmount(mount_name)

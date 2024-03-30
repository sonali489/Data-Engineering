# Databricks notebook source
dbutils.fs.unmount("/mnt/s3_mount")

# COMMAND ----------

aws_access_key = "AKIA3S3LSMAWCJKDZZ4Y"
aws_secret_key = "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"
s3_bucket_name = "buckinew"
mount_name = "s3_mount"

# Mount S3 bucket
dbutils.fs.mount(
  source = "s3a://%s:%s@%s" % (aws_access_key, aws_secret_key, s3_bucket_name),
  mount_point = "/mnt/%s" % mount_name,
  extra_configs = {"fs.s3a.access.key": aws_access_key, "fs.s3a.secret.key": aws_secret_key}
)

# Check if the mount was successful
display(dbutils.fs.ls("/mnt/%s" % mount_name))



# COMMAND ----------

df=spark.read.format("csv")\
    .option("header","true")\
        .option("inferschema","true")\
            .load("dbfs:/mnt/s3_mount/Bank Customer Churn Prediction.csv")
df.show(4)


# COMMAND ----------

df.printSchema

# COMMAND ----------

df.createOrReplaceTempView("Customer_churn")#Create a view table for sql queries

# COMMAND ----------

#Drop the unwanted columns 
df.na.drop(how="any",subset=["customer_id"]).count()

# COMMAND ----------

from pyspark.sql.functions import col, sum
#To show the gender wise churn
gen = df.groupBy("gender").agg(sum("churn")).alias("churn")
display(gen)

# COMMAND ----------

#To show the country wise churn 
con = df.groupBy("country").agg(sum("churn")).alias("churn")
display(con)

# COMMAND ----------

# Churn based on AGE
chur= df.groupBy("age").agg(sum("churn")).alias("churn")
display(chur)

# COMMAND ----------

# No of empty bank account churns
cnt=df[(df["churn"]==1) & (df["balance"]==0)].count()
display(spark.createDataFrame([(cnt,)], ["Empty bank account churn"]))

# COMMAND ----------

#Calculate the churn percentage based on each product_number
from pyspark.sql.functions import col

total_records_per_product = df.groupBy("products_number").count()
churned_records_per_product = df.filter(col("churn") == 1).groupBy("products_number").count()

joined_df = total_records_per_product.join(churned_records_per_product, "products_number", "left_outer").orderBy("products_number")

joined_df.show(4)

# COMMAND ----------

from pyspark.sql.functions import round

churn_percentage_per_product = joined_df.withColumn("churn_percentage",round((churned_records_per_product["count"]/total_records_per_product["count"])* 100,2))

churn_percentage_per_product = churn_percentage_per_product.drop("count")

display(churn_percentage_per_product)

# COMMAND ----------



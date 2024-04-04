# Databricks notebook source
display(dbutils.fs.ls('dbfs:/mnt/dbacademy-users/hrishisingh@kubrickgroup.com/data-engineer-learning-path/pipeline_demo/stream-source/orders'))

# COMMAND ----------

spark.read.format("json").load('dbfs:/mnt/dbacademy-users/hrishisingh@kubrickgroup.com/data-engineer-learning-path/pipeline_demo/stream-source/orders').show()

# COMMAND ----------

display(dbutils.fs.ls('dbfs:/mnt/dbacademy-users/hrishisingh@kubrickgroup.com/data-engineer-learning-path/pipeline_demo/storage_location/tables/orders_by_date'))

# find out how to view these parquet tables 

# COMMAND ----------

from delta.tables import DeltaTable

# Load the Delta table
delta_table = DeltaTable.forPath(spark, "dbfs:/mnt/dbacademy-users/hrishisingh@kubrickgroup.com/data-engineer-learning-path/pipeline_demo/storage_location/tables/orders_by_date")

# Display the data as a DataFrame
display(delta_table.toDF())

# COMMAND ----------

# Read the Delta table
delta_df = spark.read.format("delta").load("dbfs:/mnt/dbacademy-users/hrishisingh@kubrickgroup.com/data-engineer-learning-path/pipeline_demo/storage_location/tables/orders_by_date")

# Display the DataFrame
display(delta_df)


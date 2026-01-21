# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %sql
# MAGIC select * from parquet.`/Volumes/pyspark_python/pyspark/ext_vol/Output/sample6/`

# COMMAND ----------

# MAGIC %sql
# MAGIC describe detail mini_project.bronze.customers

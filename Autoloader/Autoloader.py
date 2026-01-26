# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog project_1;
# MAGIC use schema autoloader;

# COMMAND ----------

# created source path
dbutils.fs.mkdirs('/Volumes/project_1/autoloader/ext_vol/input/1')
dbutils.fs.mkdirs('/Volumes/project_1/autoloader/ext_vol/input/2')
dbutils.fs.mkdirs('/Volumes/project_1/autoloader/ext_vol/input/3')
dbutils.fs.mkdirs('/Volumes/project_1/autoloader/ext_vol/input/4')

# COMMAND ----------

# create checkpoint location
dbutils.fs.mkdirs('/Volumes/project_1/autoloader/ext_vol/checkpoint/autoloader')

# COMMAND ----------

df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")   # ✅ FORMAT ONLY
    .option("pathGlobFilter", "*.csv")
    .option("header", "true")
    .option("checkpointLocation","/Volumes/project_1/autoloader/ext_vol/checkpoint/autoloader/1")    # ✅ CHECKPOINT
    .option("cloudFiles.schemaLocation","/Volumes/project_1/autoloader/ext_vol/schema/autoloader" )                                     # ✅ SCHEMA
    .load("/Volumes/project_1/autoloader/ext_vol/input/")
)


# COMMAND ----------

# MAGIC %sql
# MAGIC use catalog project_1;
# MAGIC use schema autoloader;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customers (
# MAGIC     CustomerID    STRING,
# MAGIC     FullName      STRING,
# MAGIC     Email         STRING,
# MAGIC     SignUpDate    STRING,     -- kept STRING in Bronze
# MAGIC     City          STRING,
# MAGIC     Country       STRING,
# MAGIC     _rescued_data STRING,
# MAGIC     __file         STRING
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# DBTITLE 1,Cell 5
# write data to delta table from streaming df

from pyspark.sql.functions import col, lit

(
    df
    .withColumn("__file", col("_metadata.file_name"))
    .writeStream
    .option("checkpointLocation","/Volumes/project_1/autoloader/ext_vol/checkpoint/autoloader/1")
    .option("mergeSchema", True)
    .outputMode("append")
    .trigger(availableNow=True)  # batch mode
    .toTable("project_1.autoloader.customers")

)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from project_1.autoloader.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC select `__file`, count(*) from project_1.autoloader.customers
# MAGIC group by `__file`

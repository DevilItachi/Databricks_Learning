-- Databricks notebook source
-- DBTITLE 1,Cell 1
select * from parquet.`/Volumes/pyspark_python/pyspark/ext_vol/Output/sample6/` limit 5

-- COMMAND ----------

describe detail mini_project.bronze.customers

-- COMMAND ----------

describe extended mini_project.bronze.customers

-- COMMAND ----------

select current_catalog(), current_schema()

-- COMMAND ----------

describe catalog mini_project;

-- COMMAND ----------

use catalog mini_project;
describe schema bronze;


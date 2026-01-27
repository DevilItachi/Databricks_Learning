-- Databricks notebook source
-- DBTITLE 1,Cell 1
-- select data for a table from path where file is stored
select * from parquet.`/Volumes/pyspark_python/pyspark/ext_vol/Output/sample6/` limit 5

-- COMMAND ----------

describe detail mini_project.bronze.customers

-- COMMAND ----------

describe extended mini_project.bronze.customers

-- COMMAND ----------

describe history mini_project.bronze.customers

-- COMMAND ----------

select * from mini_project.bronze.customers version as of 17;

-- COMMAND ----------

select current_catalog(), current_schema()

-- COMMAND ----------

describe catalog mini_project;

-- COMMAND ----------

use catalog mini_project;
describe schema bronze;


-- COMMAND ----------

use catalog mini_project;
show tables dropped in bronze;

-- COMMAND ----------

undrop table bronze.employees

-- COMMAND ----------

insert into bronze.employees values (1,'amit')

-- COMMAND ----------

drop table bronze.employees

-- COMMAND ----------

select * from bronze.employees

-- COMMAND ----------

show tables in bronze

-- COMMAND ----------

show catalogs;


-- COMMAND ----------

show catalogs like '*in*'

-- COMMAND ----------

-- DBTITLE 1,Cell 15
use catalog mini_project;
show schemas in mini_project like '*r*';

-- COMMAND ----------

show tables in mini_project.bronze like '*ers*'

-- COMMAND ----------

describe history  mini_project.bronze.customers

-- COMMAND ----------

-- DBTITLE 1,Untitled
SELECT * FROM mini_project.bronze.customers VERSION AS OF 0

-- COMMAND ----------

create view vw_customers as select * from mini_project.bronze.customers;


-- COMMAND ----------

create temporary view tmp_customers as select * from mini_project.bronze.customers

-- COMMAND ----------

-- DBTITLE 1,Cell 21
CREATE OR REPLACE GLOBAL TEMP VIEW gb_tmp_customers AS SELECT * FROM mini_project.bronze.customers;

-- COMMAND ----------

Create table  pyspark_python.pyspark.csv_table_tmp  as select * from pyspark_python.pyspark.csv_table

-- COMMAND ----------

select * from pyspark_python.pyspark.csv_table_tmp

-- COMMAND ----------

create table pyspark_python.pyspark.csv_table_deep Deep clone pyspark_python.pyspark.csv_table;
select * from pyspark_python.pyspark.csv_table_deep

-- COMMAND ----------

create table pyspark_python.pyspark.csv_table_shallow shallow clone pyspark_python.pyspark.csv_table;

-- COMMAND ----------

select * from pyspark_python.pyspark.csv_table_shallow

-- COMMAND ----------

Alter table pyspark_python.pyspark.csv_table_deep cluster by (index_id);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.help()

-- COMMAND ----------

--copy into

create table pyspark_python.pyspark.tbl_copy_into;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df = spark.read.csv("/Volumes/project_1/autoloader/ext_vol/input/1/Customers_1.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(df)

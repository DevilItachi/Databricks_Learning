- select * from delta.`path` # this is for delta table search
- select * from parquet.`path`  # this is for parquet table search. can give json and csv too
- describe detail table;
- describe extended table;
- % is used to run magic commands
- 2 types of table - managed and external
        - managed table is created in the same location as the data.
            - Deleting manage table deletes the data and metadata. 
        - external table is created in a different location
            - Deleting external table deletes the metadata but not the data. Can undrop the table to restore the data.
        - Manage and external this can created for Catalog, schema, table, VOlumes
- select current_catalog(), current_schema()
- describe catalog mini_project
- describe schema bronze
- In Unity Catalog , if we drop a managed table, data will not get detleted immediately , but within 7-30 days.
- In Databricks Unity Catalog, dropped objects are soft-deleted, not immediately forgotten
- show tables dropped in schema;  # shows tables dropped within the retention window (≈ last 7 days)
- undrop table bronze.employees;  # table can be recovered within 7 days with full data.
- show tables in schema;          # shows all table in a schema
- show catalogs like 'eu*'        # shows catalog like eu. use * here
- show schemas in mini_project like '*r*'; 
- show tables in mini_project.bronze like '*ers*'
- describe history  mini_project.bronze.customers  # shows all operation and history on that particular table
- SELECT * FROM mini_project.bronze.customers VERSION AS OF 0   # shows data of that version ..starts from 0
- Temp and Global views
        - create view vw_customers as select * from mini_project.bronze.customers;         # normal view
        - create temporary view tmp_customers as select * from mini_project.bronze.customers # temp view
        - CREATE OR REPLACE GLOBAL TEMP VIEW gb_tmp_customers AS SELECT * FROM mini_project.bronze.customers;  # global temp view
- CTAS (Create Table As Select)
        - Create table  pyspark_python.pyspark.csv_table_tmp  as select * from pyspark_python.pyspark.csv_table; # creates new table with data
- Deep Clone
        - create table pyspark_python.pyspark.csv_table_deep Deep clone pyspark_python.pyspark.csv_table;
        - copy metadata and data both, exact copy of the source table. Delta log is copied. All data files are duplicated. Time travel preserved.
        - Source dependency NO
        - Deep clone is better than CTAS, as in CTAS there is risk of loosing metadata
- Shallow Clone
        - create table pyspark_python.pyspark.csv_table_shallow shallow clone pyspark_python.pyspark.csv_table;
        - copy metadata only. Data files are not copied. Data is referenced from source table. Time travel is not preserved.
        - Source dependency
        - It points to version of source table when we did shadow clone. If source table gets updated shadow table doesnt get updated.
        - It will still point to version of source wehn it ws created.
        - Gets impacted when source table is vaccumed.
- Vacuum
        - vacuum table pyspark_python.pyspark.csv_table_deep retain 0 hours;
        - Delta tables keep old files for time travel & rollback.
        - VACUUM is what physically cleans storage
        - Default behavior: Removes files older than 7 days (168 hours)
- Deletion Vector
        - deletion vector is a file that contains information about which rows have been deleted from a table.
        - When a table is vacuumed, the deletion vector is used to identify which data files can be deleted.
        - The deletion vector is stored in the table's metadata and is updated whenever a row is deleted from the table.
        - The table properties needs to be enabled for Deletion Vector.
        - Data is not rewritten, rather the row in parquet file will be flagged as deleted.
- Liquid Clustering
        - WHen liquid clustering is enabled on a column in table the incremental data will automatically get adjusted as per cluster column.
        - Alter table pyspark_python.pyspark.csv_table_deep cluster by (col_name);
        - Can have multiple columns
- DBUTILS
        - used for creating folder, widget, move files, list directories
- CLuster
        - compute means cluster or group or virtual macahines to work is called cluster
        - 2 types 
            1. Interactive cluster
            2. Job cluster
- Idompotent pipeline
        - The ability to execute the same operation multiple times without changing the result at target end.
- COPY INTO
        - Once you load a file via COPY INTO command , you cant reload the same file again even if you run the command multiple times.
        - This is very good option for retry & idompotent
        - Tables used for COPY INTO command doesnt have any columns to begin with.
- Autoloader
        - Autoloader is a streaming source that can be used to read data from a variety of file formats.
        - Autoloader is an utility that we can use in order to incrementally & efficiently process new files that are arriving in your cloud storage.
        - Can be used with both streaming and batch mode.
        - Uses checkpoints in order to manage & incrementally process new files.
        - File Detection mode
            - 1. Directory listing - use API calls to detect new files (DEFAULT)
            - 2. File notification - use notification & queue services 
        - If we rerun the cell again and again it wont load data multiple time. IT will load only once.
        - Handles Schema evolution
            - Scehma evoultion - addNEWColumns -- adds new column to schema
                               - rescue -- all new columns data is recorded in resecue columns
                               - none --  new columns are ignored 
                               - FailOnNewColumn - Stream fails if new columns are coming 
- Schema Managements and Secrets
        - There might be a requirement to put some credentials or sensitive url passwords or host names into your notebooks or jobs.
        - Its  not a good practice to put these as plain strings.
        - Sceret scope is somrthing where you can add your secrets and then later use them in your notebooks and jobs.
        2 types of secret scopes
            1. Azure Key Vault
            2. Databricks Secret Scope
- Users , Service principal and Groups
        - Users - individual users
        - Groups - collection of users
        - Service principal (SPN) - are used to run jobs & do automation stuff
- Databricks Unity Catalog
        - Databricks Unity Catalog is a cloud-native data governance and security platform that provides a unified view of all data assets across your organization.
- Row Level Filters in unity catalog
        - Hiding sensitive data is very imp part of data governance
        - Row level filters are used to hide sensitive data from users who are not authorized to see it.
        - Create a function & then apply this function to that table as row level filter.
- Column level masking
        - Hiding/masking full column data to specific users.
        - Create a function & then apply this function to that table 















- select * from delta.`path` # this is for delta table search
- select * from parquet.`path`  # this is for parquet table search. can give json and csv too
- select * from read_files('path', header = true, format = 'csv')
- describe detail table;
- describe extended table;
- describe history table; # shows history of table
- _delta_log is the transactional log where all details are stored. maintains JSON commit logs. Also contains checkpoint Parquet files (*.checkpoint.parquet) for performance.
- alter table table_name SET PROPERTIES (condition)
- % is used to run magic commands
- 2 types of table - managed and external
        - managed table is created in the same location as the data.
            - Deleting manage table deletes the data and metadata. 
            - Dropping a managed table removes metadata immediately, and data is deleted after retention period.
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
        - Deep clone is better than CTAS, as in CTAS there is risk of loosing metadata.
        - Time travel works from clone creation onward, not before.
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
        - Deletes files and not rows. 
- Deletion Vector
        - deletion vector is a file that contains information about which rows have been deleted from a table.
        - When a table is optimized, the deletion vector is used to identify which data files can be deleted and delete those.
        - The deletion vector is stored in the table's metadata and is updated whenever a row is deleted from the table.
        - The table properties needs to be enabled for Deletion Vector.
        - Data is not rewritten, rather the row in parquet file will be flagged as deleted.
- Liquid Clustering
        - WHen liquid clustering is enabled on a column in table the incremental data will automatically get adjusted as per cluster column.
        - Alter table pyspark_python.pyspark.csv_table_deep cluster by (col_name);
        - Can have multiple columns. 
- DBUTILS
        - used for creating folder, widget, move files, list directories
- CLuster
        - compute means A cluster is a group of VMs used to execute Spark workloads.
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
        - Autoloader is an utility that we can use in order to incrementally & efficiently process new files that are arriving in your cloud storage.
        - Autoloader is a strucutred streaming source that can be used to read data from a variety of file formats.
        - Can be used with both streaming and batch mode.
        - Uses checkpoints in order to manage & incrementally process new files.
        - Can use to load files from different folders as well.
        - File Detection mode
            - 1. Directory listing - use API calls to detect new files (DEFAULT)
            - 2. File notification - use notification & queue services 
        - If we rerun the cell again and again it wont load data multiple time. IT will load only once.
        - Schema location is also created which is used to Handles Schema evolution
        - Autoloader gives you feature to handle the schema evoulution.
            - Scehma evoultion - addNEWColumns -- adds new column to schema (default)
                               - rescue -- all new columns data is recorded in resecued columns
                               - none --  new columns are ignored , no data is rescued, stream doesnt fail
                               - FailOnNewColumn - Stream fails if new columns are coming 
        - addNewColumns when ran on 1st time it will fail as new schema is coming, now because of this new column schema is being store at schema location.and when ran nexxt time, it automatically process it.
- Schema Managements and Secrets
        - There might be a requirement to put some credentials or sensitive url passwords or host names into your notebooks or jobs.
        - Its  not a good practice to put these as plain strings.
        - Sceret scope is something where you can add your secrets and then later use them in your notebooks and jobs.
        2 types of secret scopes
            1. Azure Key Vault
            2. Databricks Secret Scope 
        - Azure key vault
                - search key vault
                - create key vault
                - once completed go to access control(IAM) and add role and search for azzure databricks
                - then create scope
                - create secrets
                - in notebooks with help of dbutils we can play with secrets. 
                - Secrets is within scope
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
- Time travel
        - Time Travel in Databricks is a Delta Lake feature that allows you to:
                - Query previous versions of a Delta table
                - Restore data after accidental DELETE / UPDATE / DROP
                - Compare before vs after states of data
                - Support auditing, debugging, and rollback
                - Every change to a Delta table creates a new version (transaction log).
        - Version number = key for Time Travel
        - 2 ways to do time travel
                - SELECT * FROM bronze.customers VERSION AS OF 3;   # just seeing data of that version
                - SELECT * FROM bronze.customers TIMESTAMP AS OF '2026-01-18 10:15:00'  # just seeing data of that version
        - Restoring the table via time travel
                - RESTORE TABLE bronze.customers TO VERSION AS OF 3; #restoring data to that version. This creates a NEW version, it does NOT delete history
        - ALTER TABLE bronze.customers  # change retention period of vaccum
                SET TBLPROPERTIES (
                  delta.logRetentionDuration = '30 days',
                  delta.deletedFileRetentionDuration = '30 days'
                );

Unity catalog
        - used for data governance
        - Data goverance means secure, available and accurate  
        - Its an open source and unified( define it once and use it everywhere)
        - Its centralized and used for security
        - used for autiditing purpose
        - Lineage and data discovery
        - without unity catalog we have to have manage eah workspace separately. 
        - 1 region should have 1 metastore
        - Catalog is data securable object .
        - First  create a metastore and then unity catalog
                - Metastore requires a cloud storage like ADLS.
                - Use access connector for azure databricks to mount the adls on databricks
                - Then give permission to connector to access the ADLS via IAM and give role.
                - Then give adls path in metastore creation
                - In end it asks for unity catalog enable .

SQL Alerts
        - Alerts help you send notification to some destination based on the sql query when a condition is met.
        - Create a sql query first and then create an alert based on that query.
        - Trigger condition is then require
        - Notification 
        - Template 
        - To evaluate click ono refresh
        - lastly add schedule and destination(emailids)






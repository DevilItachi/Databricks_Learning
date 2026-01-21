# Databricks notebook source
# MAGIC %md
# MAGIC ##Common util functions notebook
# MAGIC
# MAGIC This notebook contains various utility functions and configurations for data processing and management tasks. Below is a description of the functions and configurations used in this notebook.
# MAGIC
# MAGIC ###Widgets
# MAGIC 1. `var_env_name = dbutils.widgets.get("ENV")`
# MAGIC 2. `var_source_param = dbutils.widgets.get("JOB_NM")`
# MAGIC 3. `var_task_name = dbutils.widgets.get("TASK_NM")`
# MAGIC
# MAGIC ###Functions
# MAGIC 1. `get_latest_filename(source_adls_full_path, run_dt)`
# MAGIC    - Retrieves the latest file from the specified ADLS path based on the modification time.
# MAGIC
# MAGIC 2. `fn_src_tgt_ingestion_raw(v_load_type, v_src_adls_path, v_src_extn, v_delim, v_is_hdr, v_tgt_schema, v_tgt_tbl)`
# MAGIC    - Ingests raw data from the source to the target table based on the specified load type.
# MAGIC
# MAGIC 3. `fn_table_load(v_load_type, v_src_schema, v_src_tbl, v_tgt_schema, v_tgt_tbl, v_load_query, v_merge_cols, v_filter_end_ts)`
# MAGIC    - Loads data from the source table to the target table based on the specified load type and query.
# MAGIC
# MAGIC 4. `fn_append_ts_todate(input_dt)`
# MAGIC    - Appends the current timestamp to the given date.
# MAGIC
# MAGIC 5. `fn_parallel_full_load(row)`
# MAGIC    - Performs a parallel full load of data from the source to the target table.
# MAGIC
# MAGIC 6. `task_control_logging(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_ts, filter_end_ts, execution_start_ts, execution_end_ts, source_count, target_count, status)`
# MAGIC    - Logs task control information.
# MAGIC
# MAGIC 7. `task_run_logging(job_id, parent_run_id, task_run_id, procedure_name, task_run_id, procedure_runtime_stamp, err_msg, src_cnt, tgt_cnt)`
# MAGIC    - Logs task run information.
# MAGIC
# MAGIC 8. `fn_arch_file(v_file_path, v_archival_adls_path)`
# MAGIC    - Archives a file from the source path to the archival path.
# MAGIC
# MAGIC 9. `fn_arch_folder(v_folder_path, v_archival_adls_folder_path)`
# MAGIC     - Archives all files in a folder from the source path to the archival path.
# MAGIC
# MAGIC 10. `fn_housekeeping_files(v_folder_path, v_no_of_months=6)`
# MAGIC     - Performs housekeeping by deleting files older than the specified number of months.
# MAGIC
# MAGIC 11. `task_run_logging_sp_notebook(job_id, parent_run_id, task_run_id, procedure_name, message_type, priority, message_cd, primary_message, secondary_message, login_id, data_key, records_in, records_out, procedure_runtime_stamp, records_updated, logType, sp_exec_time, sql_seq_no, qry_Type, table_name, sql_count)`
# MAGIC     - Logs task run information for stored procedures and notebooks.
# MAGIC
# MAGIC 12. `sp_exec_minutes(s_time)`
# MAGIC     - Calculates the execution time in minutes.
# MAGIC
# MAGIC 13. `db_list_files(file_path, file_prefix)`
# MAGIC     - Lists files in the specified path with the given prefix.
# MAGIC
# MAGIC 14. `fn_arch_files(var_src_path, var_tgt_path)`
# MAGIC     - Archives files from the source path to the target path based on the file prefix
# MAGIC
# MAGIC 15. `fn_write_deltalake_to_snowflake(v_load_type,v_src_schema,v_src_tbl,v_tgt_schema,v_tgt_tbl,v_load_query,v_merge_cols,v_filter_end_ts)`
# MAGIC 	- Write data from Delta Lake to Snowflake table based on the specified load type using the values from parameter
# MAGIC
# MAGIC 16. `fn_src_tgt_ingestion_synapse_to_deltalake(v_load_type,v_src_schema,v_src_tbl,v_tgt_schema,v_tgt_tbl,v_load_query,v_merge_cols):
# MAGIC     - Function to ingest data from Synapse table to Delta Lake table
# MAGIC 	
# MAGIC 17. `sp_exec_minutes(s_time)`
# MAGIC     - Calculate the difference in minutes between the current time and the provided datetime.
# MAGIC 	
# MAGIC 18. `fn_get_adls_last_processed_file(v_catalog_param, v_schema_nm_taskctrl, v_job_name, v_task_name)`
# MAGIC     - Retrieve the last processed file for a given job task from the task control table.
# MAGIC
# MAGIC 19. `fn_get_file_info_from_adls(adls_path)`
# MAGIC     - Retrieve the list of files available in the ADLS container along with the last modified time.
# MAGIC
# MAGIC 20. `fn_ingest_all_files_from_adls(var_file_path,var_text_files_schema, var_load_type, var_catalog_param, var_schema_nm_taskctrl ,var_target_schema, var_target_table, var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding,var_source_file_extension)`
# MAGIC     - Ingest all files from ADLS into target delta table based on the file extension. If txt it get processed as fixed width else as csv
# MAGIC 	
# MAGIC 21. `fn_fixed_width_file_ingestion(fixed_width_filepath:str, fixed_width_schema:dict,v_load_type, v_catalog_param, v_schema_nm_taskctrl ,v_tgt_schema,v_tgt_tbl, v_task_name, v_job_name, v_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding)`
# MAGIC     - Ingest fixed-width files from ADLS into target delta table
# MAGIC 	
# MAGIC 22. `update_task_control_restart(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status,last_processed_file='NULL')`
# MAGIC     - Update the task control table with the restart information using parameter values. Raise exception if update fails with exception or reached the max retries incase of cuncurrent execution exception
# MAGIC  
# MAGIC 23. `update_task_control(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status,last_processed_file='NULL')`
# MAGIC     - Update the task control table with the restart information using parameter values without last processed file parameter. Raise exception if update fails with exception or reached the max retries incase of cuncurrent execution exception
# MAGIC
# MAGIC 24. `get_filter_start_ts(var_job_name,var_task_name)`    
# MAGIC     - Get the max filter end timestamp for a given job and task to use as filter start time for new run.
# MAGIC 	
# MAGIC 25. `fn_retry_query(v_query,max_retries:int = 3)`
# MAGIC     - Excute SQL query with retry in case of concurrent error.

# COMMAND ----------

###############################################################################
## Script Name    : nb_cmn_env_config                                         #
## Creation Date  : 05-23-2024                                                #                    
## Created by     : Chubb LDM - Cognizant                                     #
## Purpose        : This script is used to                                    #
##                  1. Required util function                                 #
###############################################################################

# COMMAND ----------

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
spark.conf.set("spark.databricks.delta.rowLevelConcurrencyPreview", "true")

# COMMAND ----------

import pyspark
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime  
import sys
from pyspark.errors import PySparkException
import shutil
import os
import time
import re
from delta.exceptions import ConcurrentAppendException
import random

# from pyspark.sql.functions import monotonically_increasing_id

# var_metadata_tbl = 'TBL_METADATA_CONFIG_DETAILS'
# var_task_control = 'TBL_TASK_CONTROL'
# var_task_run_log = 'TBL_TASK_RUN_LOG'

# dbutils.widgets.text("ENV","")
var_env_name=dbutils.widgets.get("ENV")
# dbutils.widgets.text("JOB_NM","")
var_source_param=dbutils.widgets.get("JOB_NM")
var_task_name=dbutils.widgets.get("TASK_NM")

current_session_user = spark.sql(f'select current_user()').collect()[0][0]

def get_latest_filename(source_adls_full_path, run_dt):
        #################################################################
        #     Get the latest file from the given ADLS path based on modification time.

        #     Parameters:
        #     source_adls_full_path (str): The full path to the ADLS source.
        #     run_dt (str): The run date to filter files.

        #     Returns:
        #     str: The path of the latest file or "No File Found" if no file is found.
        #################################################################

        files_df = spark.read.format("binaryFile").load(source_adls_full_path)

        # Extract the file name and modification time
        files_df = files_df.select(col("path"), col("modificationTime"))

        # Identify the latest file
        if run_dt != '':
                files_df = files_df.filter(files_df.path.like("%{}%".format(run_dt)))


        latest_file_df = files_df.orderBy(col("modificationTime").desc()).limit(1)
        if latest_file_df.count() == 0:
                var_latest_filename = "No File Found"
        else:
                var_latest_filename = latest_file_df.collect()[0][0]

        return var_latest_filename

def fn_src_tgt_ingestion_raw(v_load_type,v_src_adls_path,v_src_extn,v_delim,v_is_hdr,v_tgt_schema,v_tgt_tbl):
        #################################################################
        #     Ingest raw data from ADLS to a target table in Delta Lake. Baded on load type, soruce path, extension, delimiter, header, target schema and table name parameters.

        #     Parameters:
        #     v_load_type (str): The type of load (UPSERT, OVERWRITE_ONLY_LOAD_BY, APPEND, OVERWRITE).
        #     v_src_adls_path (str): The source ADLS path.
        #     v_src_extn (str): The file extension of the source data.
        #     v_delim (str): The delimiter used in the source data.
        #     v_is_hdr (str): Whether the source data has a header (True/False).
        #     v_tgt_schema (str): The target schema.
        #     v_tgt_tbl (str): The target table.

        #     Returns:
        #     tuple: Source count, target count, and execution end time.

        #################################################################
        #print(v_load_type,v_src_adls_path,v_src_extn,v_delim,v_is_hdr,v_tgt_schema,v_tgt_tbl)
        # Reading raw data into dataframe
        df_src = spark.read.format(f"{v_src_extn}")\
                                .option("header" ,f"{v_is_hdr}")\
                                .option("inferSchema", "False")\
                                .load(v_src_adls_path,sep=f"{v_delim}")
       
        var_src_cnt=df_src.count()
        #print("File reading is successful!")
        # Load file data into  temp view as a place holder 
        df_src.createOrReplaceTempView(v_tgt_tbl+'_tmp')
        #print(v_src_adls_path , ' file count : ',df_src.count())
        var_tgt_tbl_cnt=df_src.count()
        execution_end_time = datetime.datetime.now().replace(microsecond=0)
        
        if v_load_type.upper() == 'UPSERT':
                v_sql_qry=""" INSERT INTO  """+var_catalog_param+"""."""+v_tgt_schema+"""."""+v_tgt_tbl+""" 
                                SELECT  *,  '"""+ var_task_name + """' CRT_BY,
                          '"""+ str(execution_end_time) + """' CRT_TS,
                          '"""+ var_task_name + """' UPD_BY,
                        '"""+ str(execution_end_time) + """' UPD_TS
                                FROM """+ v_tgt_tbl+"""_tmp""" 
        elif v_load_type.upper() == 'OVERWRITE_ONLY_LOAD_BY':
                v_sql_qry= f"""INSERT OVERWRITE   {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY
                                FROM {v_tgt_tbl}_tmp""" 

        elif v_load_type.upper() == 'APPEND':
                v_sql_qry= f"""INSERT INTO   {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS
                                FROM {v_tgt_tbl}_tmp"""                                

        else:
                if v_load_type.upper() == 'OVERWRITE':
                        v_sql_qry = f"""INSERT OVERWRITE {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS 
                                FROM {v_tgt_tbl}_tmp""" 
        #print(v_sql_qry)
        spark.sql(v_sql_qry)
        df_tgt_cnt = spark.sql(f"describe history {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} limit 1")
        var_tgt_cnt = df_tgt_cnt.select('operationMetrics').collect()[0][0]['numOutputRows']
        #df_tgt_cnt_qry = f"SELECT * FROM {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}"
        #var_tgt_cnt = spark.sql(df_tgt_cnt_qry).count()
        #print("File loading to table is successful!")

        return var_src_cnt, var_tgt_cnt,execution_end_time
          

#function for full load
def fn_table_load(v_load_type,v_src_schema,v_src_tbl,v_tgt_schema,v_tgt_tbl,v_load_query,v_merge_cols,v_filter_end_ts):
        ########################################################################
        # Load data from source table to target table in Delta Lake based on the type of load, schema and table from parameter.

        # Parameters:
        # v_load_type (str): The type of load (UPSERT, OVERWRITE, APPEND).
        # v_src_schema (str): The source schema.
        # v_src_tbl (str): The source table.
        # v_tgt_schema (str): The target schema.
        # v_tgt_tbl (str): The target table.
        # v_load_query (str): The load query.
        # v_merge_cols (str): The merge columns.
        # v_filter_end_ts (str): The filter end timestamp.

        # Returns:
        # tuple: Source count and target count.
        ########################################################################

        #print( v_src_schema,'.', v_src_tbl,' load started to  ',v_tgt_schema,'.',v_tgt_tbl)
        var_src_cnt = 0
        index = v_load_query.lower().find("select")
        var_src_cnt_qry = v_load_query[index:]
        if v_load_type.upper() == 'UPSERT':
                #print("inside merge statement")
                #var_src_cnt = 0
                #print(v_merge_cols)
                if v_merge_cols is not None and v_merge_cols != '' and v_merge_cols != 'null':
                        targt_lst_updtd_max_ts_qury = f'SELECT MAX({v_merge_cols}) FROM {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}'
                        targt_lst_updtd_max_ts = spark.sql(targt_lst_updtd_max_ts_qury).collect()[0][0] #
                        if targt_lst_updtd_max_ts == None:      # this applicable only for the first time load
                                targt_lst_updtd_max_ts = '1990-01-01T00:00:00Z' 
                        #print(targt_lst_updtd_max_ts)
                        df_src_cnt_qry = f" SELECT * FROM {var_catalog_param}.{v_src_schema}.{v_src_tbl} WHERE {v_merge_cols} > to_timestamp('{targt_lst_updtd_max_ts}') "
                        #print(df_src_cnt_qry)
                        var_src_cnt = spark.sql(df_src_cnt_qry).count()
                        #print(var_src_cnt)
                        if var_src_cnt > 0:
                                v_sql_qry_final = v_load_query.format(targt_lst_updtd_max_ts)
                                #print(v_sql_qry_final)
                                spark.sql(v_sql_qry_final)
                        else:
                                #print('============= NO NEW OR UPDATED RECORDS FOUND IN THE SOURCE TABLE =============')
                                var_src_cnt = var_tgt_cnt = 0
                else:
                        df_src_cnt_qry = f" SELECT * FROM {var_catalog_param}.{v_src_schema}.{v_src_tbl} "
                        #print(df_src_cnt_qry)
                        var_src_cnt = spark.sql(df_src_cnt_qry).count()
                        v_sql_qry_final = v_load_query
                        #print(v_sql_qry_final)     
                        spark.sql(v_sql_qry_final)
        else:
                if v_load_type.upper() == 'OVERWRITE':
                        #print("--------------Loading table------------ ")
                        # v_sql_qry=f"TRUNCATE TABLE {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}"
                        # spark.sql(v_sql_qry)
                        if v_load_query is not None:
                                v_load_query = f"{v_load_query}"
                        else:
                                v_load_query = f"SELECT * FROM {var_catalog_param}.{v_src_schema}.{v_src_tbl}"
                        #print("Load Query : ",v_load_query)
                        # var_src_cnt = spark.sql(v_load_query).count()
                        var_src_cnt = spark.sql(var_src_cnt_qry).count()
                        v_sql_qry=f""" INSERT OVERWRITE {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} {v_load_query}"""
                        spark.sql(v_sql_qry)
                        #print( v_src_schema,'.', v_src_tbl,' load completed to ',v_tgt_schema,'.',v_tgt_tbl)
                else:
                        # var_src_cnt = spark.sql(f"{v_load_query}").count()
                        var_src_cnt = spark.sql(f"{var_src_cnt_qry}").count()
                        v_sql_qry=f""" INSERT INTO {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} {v_load_query}"""
                        spark.sql(v_sql_qry)
                        #print( v_src_schema,'.', v_src_tbl,' load completed to ',v_tgt_schema,'.',v_tgt_tbl)

        df_tgt_cnt = spark.sql(f"describe history {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} limit 1")
        var_tgt_cnt = df_tgt_cnt.select('operationMetrics').collect()[0][0]['numOutputRows']
        
        return var_src_cnt, var_tgt_cnt


#This function is used to get time part appended to given date, for backdated txns
def fn_append_ts_todate(input_dt): 
        ###############################################################
        # Append the current timestamp to the given date.

        # Parameters:
        # input_dt (str): The input date.

        # Returns:
        # str: The input date with the current timestamp appended.
        ###############################################################
        df_end_ts=spark.sql("select current_timestamp() as end_ts")
        var_upd_tms= df_end_ts.select('end_ts').collect()[0][0]
        return input_dt[:10]+ str(var_upd_tms)[10:]

# def fn_parallel_full_load(row):
#         ##################################################
#         # Perform parallel full load for a given row.

#         # Parameters:
#         # row (Row): The input row.
#         ##################################################
#         #print(time.time() ,":", row['prcs_nm']) 
#         var_src_stg_schema= row['raw_stg_schema']
#         var_src_stg_tbl= row['raw_stg_tbl']
#         fn_table_load('Append',f'raw_{var_env_name}_prima','metadata_config_details'+'1',f'raw_{var_env_name}_prima','metadata_config_details'+'_temp3','*')

def task_control_logging(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_ts,filter_end_ts,execution_start_ts,execution_end_ts, source_count, target_count,status):
        ##################################################
        # Log task control information using parameter values..

        # Parameters:
        # job_name (str): The job name.
        # job_id (str): The job ID.
        # parent_run_id (str): The parent run ID.
        # task_name (str): The task name.
        # task_run_id (str): The task run ID.
        # filter_start_ts (str): The filter start timestamp.
        # filter_end_ts (str): The filter end timestamp.
        # execution_start_ts (str): The execution start timestamp.
        # execution_end_ts (str): The execution end timestamp.
        # source_count (int): The source count.
        # target_count (int): The target count.
        # status (str): The status.
        ##################################################
        #print(f"--------Logging started in {var_task_control} -----------")
        query = f'''INSERT INTO {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control}
                (job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_ts, filter_end_ts, execution_start_ts, execution_end_ts, source_count, target_count, status)
                VALUES ('{job_name}', '{job_id}', '{parent_run_id}', '{task_name}', '{task_run_id}', {filter_start_ts}, {filter_end_ts}, {execution_start_ts}, {execution_end_ts}, {source_count}, {target_count}, '{status}')'''
        #print(query)
        spark.sql(query)
        #print(f"--------Logging ended in {var_task_control} -----------")

def task_run_logging(job_id, parent_run_id, procedure_name, task_run_id, procedure_runtime_stamp, err_msg, src_cnt, tgt_cnt):
        ##################################################################################
        # Log task run information using parameter values.

        # Parameters:
        # job_id (str): The job ID.
        # parent_run_id (str): The parent run ID.
        # task_run_id (str): The task run ID.
        # procedure_name (str): The procedure name.
        # procedure_runtime_stamp (str): The procedure runtime timestamp.
        # err_msg (str): The error message.
        # src_cnt (int): The source count.
        # tgt_cnt (int): The target count.
        ##################################################################################
        #print(f"--------Logging started in {var_task_run_log} -----------")
        query = f'''INSERT INTO {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_run_log}
                (job_id, parent_run_id, task_run_id, procedure_name, create_dt, message_type, priority, message_cd, primary_message, secondary_message, login_id, data_key, records_in, records_out, procedure_runtime_stamp, records_updated)
                VALUES ({job_id}, {parent_run_id}, {task_run_id},{procedure_name}, current_timestamp(),NULL,NULL,NULL, "{err_msg}",NULL,NULL,NULL,{src_cnt},{tgt_cnt},{procedure_runtime_stamp},NULL)'''
        #print(query)
        spark.sql(query)
        #print(f"--------Logging ended in {var_task_run_log} -----------")

def fn_arch_file(v_file_path,v_archival_adls_path):
        #################################################################
        # Archive a file from the source path to the archival path.

        # Parameters:
        # v_file_path (str): The source file path.
        # v_archival_adls_path (str): The archival ADLS path.

        # Returns:
        # tuple: Source count and target count.
        #################################################################
        #print(v_file_path ,'  ',v_archival_adls_path)
        file_list = [file.path for file in dbutils.fs.ls(v_file_path)]
        var_src_cnt=len(file_list)
        var_tgt_cnt=var_src_cnt
        # Move the file to the archival path
        dbutils.fs.mv(v_file_path, v_archival_adls_path) 
        return var_src_cnt, var_tgt_cnt

def fn_arch_folder(v_folder_path,v_archival_adls_folder_path):
        #################################################################
        # Archive all files from the source folder to the archival folder.

        # Parameters:
        # v_folder_path (str): The source folder path.
        # v_archival_adls_folder_path (str): The archival ADLS folder path.

        # Returns:
        # tuple: Source count and target count.
        #################################################################
        #print('fn_arch_folder Archiving:',v_folder_path ,'  ',v_archival_adls_folder_path)
        file_list = [file.path for file in dbutils.fs.ls(v_folder_path)]
        var_src_cnt=len(file_list)
        var_tgt_cnt=var_src_cnt
        archive_cnt=0
        for file in file_list:
                #print('Archiving File:'+ file)
                archive_cnt=archive_cnt+1
                dbutils.fs.mv(file, os.path.join(v_archival_adls_folder_path,os.path.basename(file)))
                #print('**File Archived**')
        var_tgt_cnt=archive_cnt
        #print("'fn_arch_folder Folder arvhived -----------",var_src_cnt,'',var_tgt_cnt)
        return var_src_cnt, var_tgt_cnt


def fn_housekeeping_files(v_folder_path ,v_no_of_months=6):
        #################################################################
        # Perform housekeeping by deleting files older than a specified number of months.

        # Parameters:
        # v_folder_path (str): The folder path.
        # v_no_of_months (int): The number of months to retain files (default is 6).

        # Returns:
        # tuple: Source count and target count.
        #################################################################
        #print('fn_housekeeping_folder :', v_folder_path)
        mnths_to_days = v_no_of_months*30
        seconds = time.time() - (mnths_to_days * 24 * 60 * 60) #for Deleting files before last 6 months default
        file_list = [file.path for file in dbutils.fs.ls(v_folder_path)]        
        delete_cnt=0
        for file in file_list:
                test = dbutils.fs.ls(file)
                test_time=((test[0].modificationTime)/1000)
                if seconds > test_time :
                         dbutils.fs.rm(file) #deleting files
                         delete_cnt=delete_cnt+1
        var_tgt_cnt=delete_cnt
        var_src_cnt=delete_cnt
        #print(f"Housekeeping for Folder completed removed {delete_cnt} files -----------,{var_src_cnt},{var_tgt_cnt}")
        return var_src_cnt, var_tgt_cnt

def task_run_logging_sp_notebook(job_id, parent_run_id, task_run_id, procedure_name, message_type, priority, message_cd, primary_message, secondary_message, login_id, data_key, records_in, records_out, procedure_runtime_stamp, records_updated,
                                 logType,sp_exec_time,sql_seq_no,qry_Type,table_name,sql_count):
        #################################################################
        # Log task run information for stored procedure/child notebook using the value from parameter.

        # Parameters:
        # job_id (str): The job ID.
        # parent_run_id (str): The parent run ID.
        # task_run_id (str): The task run ID.
        # procedure_name (str): The procedure name.
        # message_type (str): The message type.
        # priority (str): The priority.
        # message_cd (str): The message code.
        # primary_message (str): The primary message.
        # secondary_message (str): The secondary message.
        # login_id (str): The login ID.
        # data_key (str): The data key.
        # records_in (int): The number of records in.
        # records_out (int): The number of records out.
        # procedure_runtime_stamp (str): The procedure runtime timestamp.
        # records_updated (int): The number of records updated.
        # logType (str): The log type.
        # sp_exec_time (float): The stored procedure execution time.
        # sql_seq_no (int): The SQL sequence number.
        # qry_Type (str): The query type.
        # table_name (str): The table name.
        # sql_count (int): The SQL count.
        #################################################################
        #print(f"--------Logging started in {var_task_run_log} -----------")

        emsg = primary_message

        if  str(emsg).strip() == "" and logType.strip().lower() != "fail"  :
                if logType.strip().lower() == "progress":
                        emsg='Sql_seq: '+str(sql_seq_no)+ ', No of records ' + qry_Type + ' in the  ' +table_name +' table is '+str(sql_count)

                if logType.strip().lower() == "success":
                        emsg ='Success.Minutes to execute  sp: '+ str(sp_exec_time)  

        if logType.strip().lower() == "fail":
                error_message = primary_message
                emsg=('Procedure Failed : '+ procedure_name +   ' at the transaction no : '+str(sql_seq_no)+ ', while doing  ' + qry_Type + ' for the table ' +table_name + ',  Error_description : '+type(error_message).__name__
        +  ", " +str(error_message.args))+" " +error_message.__class__.__name__ 
                emsg=emsg.replace("'","''")
        
        query = f'''INSERT INTO {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_run_log}
                        (job_id, parent_run_id, task_run_id, procedure_name, create_dt, message_type, priority, message_cd, primary_message, secondary_message, login_id, data_key, records_in, records_out, procedure_runtime_stamp, records_updated)
                        VALUES ('{job_id}', '{parent_run_id}', '{task_run_id}','{procedure_name}', current_timestamp(),'{message_type}'
                        ,CAST('{priority}' AS DECIMAL(38,0)),'{message_cd}', '{emsg}','{secondary_message}','{login_id}','{data_key}'
                        ,CAST('{records_in}' AS DECIMAL (38,0))
                        ,CAST('{records_out}' AS DECIMAL(38,0))
                        ,'{procedure_runtime_stamp}'
                        ,CAST('{records_updated}' AS DECIMAL(38,0))

                        )'''
        #print(query)
        spark.sql(query)
        #print(f"--------Logging ended in {var_task_run_log} -----------")


def sp_exec_minutes(s_time):
        #################################################################
        # Calculate the difference in minutes between the current time and a given time.

        # Args:
        # s_time (datetime): The start time.

        # Returns:
        # str: The difference in minutes formatted to two decimal places.
        #################################################################
        e_time = datetime.now()
        time_diff = e_time-s_time 
        #print('Difference: ', time_diff)
        min_diff =  "{:.2f}".format(time_diff.total_seconds() / 60.00)
        return min_diff

def db_list_files(file_path, file_prefix):
        #################################################################
        # List files in a given directory that start with a specified prefix.

        # Args:
        # file_path (str): The path to the directory.
        # file_prefix (str): The prefix to filter files.

        # Returns:
        # list: A list of file paths that match the prefix.
        #################################################################
        file_list = [file.path for file in dbutils.fs.ls(file_path) if os.path.basename(file.path).startswith(file_prefix)]
        return file_list



def fn_arch_files(var_src_path,var_tgt_path):
        #################################################################
        # Archive files from a source path to a target path based on a prefix derived from the source path.

        # Args:
        # var_src_path (str): The source path containing files to be archived.
        # var_tgt_path (str): The target path where files will be moved.
        #################################################################

        #file_prefix="info"
        file_prefix_lst=var_src_path.split('/')
        
        file_prefix=file_prefix_lst[-1]
        file_path=var_src_path.replace(file_prefix,'')
        
        file_prefix1=file_prefix.split('*')
        file_prefix=str(file_prefix1[0])
        
        #print(file_path)
        #print(file_prefix)


        files = db_list_files(file_path, file_prefix)

        for file in files:
                #print(file)        
                dbutils.fs.mv(file, os.path.join(var_tgt_path, os.path.basename(file)))
                #print(file + " File copied to : " + var_tgt_path)
                #print(" File copied to target path ")


# COMMAND ----------

def fn_write_deltalake_to_snowflake(v_load_type,v_src_schema,v_src_tbl,v_tgt_schema,v_tgt_tbl,v_load_query,v_merge_cols,v_filter_end_ts):
        ########################################################################
        # Write data from Delta Lake to Snowflake table based on the specified load type using the values from parameter
        #
        # Parameters:
        # v_load_type (str): The type of load (UPSERT, DML, OVERWRITE, APPEND).
        # v_src_schema (str): The source schema.
        # v_src_tbl (str): The source table.
        # v_tgt_schema (str): The target schema.
        # v_tgt_tbl (str): The target table.
        # v_load_query (str): The load query.
        # v_merge_cols (str): The merge columns.
        # v_filter_end_ts (str): The filter end timestamp.
        #
        # Returns:
        # tuple: Source count and target count.
        ########################################################################
        var_src_cnt=0
        var_tgt_cnt=0
        cs = var_obj_sfcon.cursor()

        #print("Connection established !")
        #print( v_src_schema,'.', v_src_tbl,' load started to  ',v_tgt_schema,'.',v_tgt_tbl)
        
        if (v_load_query is not None and v_load_type.upper() == "DML"):
            df_src_qry = f"{v_load_query}"    

        elif (v_load_query is not None and v_load_type.upper() != "DML"):
            df_src_qry = f"{v_load_query}"

        else:
            df_src_qry = f"SELECT * FROM {var_catalog_param}.{v_src_schema}.{v_src_tbl}"

        #print(df_src_qry)

        if v_load_type.upper() == 'UPSERT':
                #print("inside merge statement")  
                raise Exception(F'**USER_DEF_EXCEPTION:Load_type -{v_load_type.upper()}, not support at the moment**********')
        else:  
                if v_load_type.upper() == 'DML':
                        df_src_qry_cnt = v_load_query.upper().replace('DELETE','select count(1) row_cnt ')
                        #print(df_src_qry_cnt)
                        df = spark.read.format(var_str_sfSoruceFromat).options(**var_dic_dfsfOptions).option("sfSchema", v_tgt_schema).option("query", f"{df_src_qry_cnt}").load()
                        #print(" get the count")
                        #print(df)
                        var_src_cnt = df.select('row_cnt').collect()[0][0]
                        #print(f"source count: {var_src_cnt}")
                        #var_tgt_cnt = var_src_cnt 
                        var_snow_query = f"{df_src_qry}"
                        #print(var_snow_query)
                        res=cs.execute(var_snow_query)
                        #print(res)
                        #print(res.rowcount)
                        var_tgt_cnt = res.rowcount
                else:
                        var_current_time=spark.sql("select current_timestamp()").collect()[0][0]
                        df_result  = spark.sql(df_src_qry)
                        var_src_cnt = df_result.count()
                        if v_load_type.upper() == 'OVERWRITE':
                               
                                df_result.write.format(var_str_sfSoruceFromat).options(**var_dic_dfsfOptions).option("sfSchema", v_tgt_schema).option("dbtable", v_tgt_tbl).mode("overwrite").save()


                        elif v_load_type.upper() == 'APPEND':
                                #print("Writing data to snowflake through Append mode")
                                #For inserting records in snowflake table 
                                df_result.write.format(var_str_sfSoruceFromat).options(**var_dic_dfsfOptions).option("sfSchema", v_tgt_schema).option("dbtable", v_tgt_tbl).mode("append").save()

                        #print(f"{v_src_schema}'.'{v_src_tbl} ' {v_load_type} load completed to snowflake table : '{v_tgt_schema}'.'{v_tgt_tbl}")
                        #df = spark.read.format(var_str_sfSoruceFromat).options(**var_dic_dfsfOptions).option("sfSchema", v_tgt_schema).option("query", f" select count(1) row_cnt from {v_tgt_schema}.{v_tgt_tbl} where to_char(LOAD_TS,'YYYYMMDD' ) = TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD')").load()
                        if v_load_type.upper() == 'OVERWRITE':
                                df = spark.read.format(var_str_sfSoruceFromat).options(**var_dic_dfsfOptions).option("sfSchema", v_tgt_schema).option("query", f" select count(1) row_cnt from {v_tgt_schema}.{v_tgt_tbl} ").load()
                        #var_current_time=spark.sql("select current_timestamp()").collect()[0][0]
                        if v_load_type.upper() == 'APPEND':
                                df = spark.read.format(var_str_sfSoruceFromat).options(**var_dic_dfsfOptions).option("sfSchema", v_tgt_schema).option("query", f" select count(1) row_cnt from {v_tgt_schema}.{v_tgt_tbl}  where LOAD_TS>='{var_current_time}' ").load()
                        #print("get the count")
                        var_tgt_cnt = df.select('row_cnt').collect()[0][0]               
                        #print(f"var_src_cnt : {var_src_cnt}")
                        #print(f"var_tgt_cnt : {var_tgt_cnt}")

        return var_src_cnt, var_tgt_cnt


# COMMAND ----------

def fn_src_tgt_ingestion_synapse_to_deltalake(v_load_type,v_src_schema,v_src_tbl,v_tgt_schema,v_tgt_tbl,v_load_query,v_merge_cols):
    ###############################################################
    # Function to ingest data from Synapse table to Delta Lake table
    # Parameters:
    # v_load_type (str): The type of load (OVERWRITE, APPEND)
    # v_src_schema (str): The source schema name in Synapse
    # v_src_tbl (str): The source table name in Synapse
    # v_tgt_schema (str): The target schema name in Delta Lake
    # v_tgt_tbl (str): The target table name in Delta Lake
    # v_load_query (str): The custom load query to retrieve data from Synapse table (optional)
    # v_merge_cols (str): The columns to use for merge operation (optional)
    ###############################################################
    var_tgt_cnt=0

    # Reading data from Synapse table
    if v_load_query is not None:
        df_src_qry = f"{v_load_query}"
    else:
        df_src_qry = f"SELECT * FROM {v_src_schema}.{v_src_tbl}"
    var_synapse_data_df = spark.read.format("com.databricks.spark.sqldw") \
                                .options(**var_str_synapseOptions) \
                                .option("query", df_src_qry) \
                                .load()

    var_current_time=spark.sql("select current_timestamp()").collect()[0][0]
    
    # Saving the data to a temporary Delta Lake table
    spark.sql(f"""drop table if exists {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}_temp""") 
    var_synapse_data_df.write.format("delta").mode("overwrite").saveAsTable(f"{var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}_temp")
    var_src_cnt = spark.sql(f"""select count(*) from  {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}_temp """).collect()[0][0]

    if v_load_type.upper() in ('OVERWRITE','APPEND'):
        if v_load_type.upper() == 'OVERWRITE':
            # Overwriting the target Delta Lake table with the data from the temporary table
            spark.sql(f""" INSERT OVERWRITE {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} SELECT *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS FROM {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}_temp""")
            
        else:
            # Appending the data from the temporary table to the target Delta Lake table
            spark.sql(f""" INSERT INTO {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} SELECT *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS FROM {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}_temp""")
        var_tgt_cnt = spark.sql(f""" SELECT COUNT(*) from {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} where LOAD_TS>='{var_current_time}'  """).collect()[0][0]
        
        # Clearing cache and dropping the temporary table
        spark.sql(f"""uncache table {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}_temp""")
        spark.sql(f"""drop table if exists {var_catalog_param}.{v_tgt_schema}.{v_tgt_tbl}_temp""")
    
    return var_src_cnt, var_tgt_cnt

# COMMAND ----------

def sp_exec_minutes(s_time):
    ##########################################################################
    # Calculate the difference in minutes between the current time and the provided datetime.

    # Parameters:
    # s_time (datetime): The datetime.

    # Returns:
    # str: The difference in minutes formatted to two decimal places.
    ##########################################################################
    e_time = datetime.now()
    time_diff = e_time-s_time    
    min_diff =  "{:.2f}".format(time_diff.total_seconds() / 60.00)
    return min_diff

def convertMillis(millisecond:int):
    ##########################################################################
    # Convert milliseconds to hours, minutes, and seconds.

    # Parameters:
    # millisecond (int): The time in milliseconds.

    # Returns:
    # tuple: A tuple containing hours, minutes, and seconds.
    ##########################################################################
    hours=int(millisecond/(1000 * 60 * 60))
    minutes=int(millisecond / (1000 * 60)) % 60
    seconds = int(millisecond / 1000) % 60
    return hours, minutes, seconds

# COMMAND ----------

# Get last ingested file from the task control table for job task
def fn_get_adls_last_processed_file(v_catalog_param, v_schema_nm_taskctrl, v_job_name, v_task_name):
    ##########################################################################
    # Retrieve the last processed file for a given job task from the task control table.
    #
    # Parameters:
    # v_catalog_param (str): The catalog parameter.
    # v_schema_nm_taskctrl (str): The schema name of the task control table.
    # v_job_name (str): The job name.
    # v_task_name (str): The task name.
    #
    # Returns:
    # str: The name of the last processed file. Returns 'NULL' if exception.
    ##########################################################################
    
    try:
        return  spark.sql(f"""  select 
                                    last_processed_file --, *                                    
                                from
                                (
                                    select
                                        *,
                                        row_number() over (
                                        partition by task_name
                                        order by execution_start_ts desc
                                        ) as row_no
                                    from
                                        {v_catalog_param}.{v_schema_nm_taskctrl}.{var_task_control}
                                        where trim(job_name) = trim('{v_job_name}') and trim(task_name) = trim('{v_task_name}')
                                        and status = 'COMPLETED'
                                )
                                where
                                    row_no = 1
                        """).collect()[0][0]
    except:
        return 'NULL'

# COMMAND ----------

def fn_get_file_info_from_adls(adls_path):
    ##########################################################################
    # Retrieve the list of files available in the ADLS container along with the last modified time.
    #
    # Parameters:
    # adls_path (str): The path to the ADLS container.
    #
    # Returns:
    # DataFrame: A DataFrame containing the list of files with their details,
    #            including a timestamp column converted from modificationTime.
    ##########################################################################
    file_list = dbutils.fs.ls(adls_path)
    file_list_df = spark.createDataFrame(file_list)
    file_list_df = file_list_df.withColumn("timestamp",to_timestamp(file_list_df['modificationTime']/1000))
    # file_list_df1 = file_list_df.filter(file_list_df.size>0) #/* size > 0 order by modificationTime */
    final_file_list_df = file_list_df.sort('modificationTime')
    return final_file_list_df

# COMMAND ----------

def fn_ingest_all_files_from_adls(var_file_path,var_text_files_schema, var_load_type, var_catalog_param, var_schema_nm_taskctrl ,var_target_schema, var_target_table, var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding,var_source_file_extension):
    ##########################################################################
    # Ingest all files from ADLS into target delta table based on the file extension. If txt it get processed as fixed width else as csv
    #
    # Parameters:
    # var_file_path (str): The path to the files in ADLS.
    # var_text_files_schema (dict): The schema for fixed-width text files.
    # var_load_type (str): The type of load operation (e.g., 'APPEND').
    # var_catalog_param (str): The catalog parameter.
    # var_schema_nm_taskctrl (str): The schema name of the task control table.
    # var_target_schema (str): The target schema name.
    # var_target_table (str): The target table name.
    # var_task_name (str): The task name.
    # var_job_name (str): The job name.
    # var_task_run_id (str): The task run ID.
    # filter_prev_end_ts (str): The previous end timestamp for filtering files.
    # var_skip_rows (int): The number of rows to skip in the file.
    # var_file_encoding (str): The file encoding type.
    # var_source_file_extension (str): The source file extension (e.g., 'txt', 'csv').
    #
    # Returns:
    # tuple: A tuple containing the source count, target count, execution end time,
    #        last processed file, and a message indicating the files processed.
    ##########################################################################
    if var_source_file_extension == 'txt':
        var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed = fn_fixed_width_file_ingestion(var_file_path,var_text_files_schema, var_load_type, var_catalog_param, var_schema_nm_taskctrl ,var_target_schema, var_target_table, var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding)
    elif var_source_file_extension == 'csv':
        var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed = fn_csv_file_ingestion(var_load_type,var_file_path,var_source_file_extension,var_source_file_delimiter,var_header,var_target_schema,var_target_table,var_catalog_param, var_schema_nm_taskctrl,var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows)
    return var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed

# COMMAND ----------

def fn_fixed_width_file_ingestion(fixed_width_filepath:str, fixed_width_schema:dict,v_load_type, v_catalog_param, v_schema_nm_taskctrl ,v_tgt_schema,v_tgt_tbl, v_task_name, v_job_name, v_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding):
    ##########################################################################
    # Ingest fixed-width files from ADLS into target delta table
    #
    # Parameters:
    # fixed_width_filepath (str): The path to the fixed-width files in ADLS.
    # fixed_width_schema (dict): The schema for fixed-width text files.
    # v_load_type (str): The type of load operation (e.g., 'APPEND').
    # v_catalog_param (str): The catalog parameter.
    # v_schema_nm_taskctrl (str): The schema name of the task control table.
    # v_tgt_schema (str): The target schema name.
    # v_tgt_tbl (str): The target table name.
    # v_task_name (str): The task name.
    # v_job_name (str): The job name.
    # v_task_run_id (str): The task run ID.
    # filter_prev_end_ts (str): The previous end timestamp for filtering files.
    # var_skip_rows (int): The number of rows to skip in the file.
    # var_file_encoding (str): The file encoding type.
    #
    # Returns:
    # tuple: A tuple containing the source count, target count, execution end time,
    #        last processed file, and a message indicating the files processed.
    ##########################################################################
    files_info = fn_get_file_info_from_adls(fixed_width_filepath)
    files_info.createOrReplaceTempView("files_info_temp")
    last_processed_file = fn_get_adls_last_processed_file(v_catalog_param, v_schema_nm_taskctrl,v_job_name,v_task_name)
    if last_processed_file is None or last_processed_file=='NULL':
        last_processed_file = re.sub("[',\],\[,']","",str(fixed_width_filepath.split('/')[-1:]))+'_19000101000000.txt'
    
    if 'monthly' in fixed_width_filepath:
        monthly_files_df = files_info.filter(col("path").contains("monthly"))
        # Get the latest file based on modification time
        latest_file = monthly_files_df.orderBy(col("modificationTime").desc()).limit(1).collect()[0].name
    try:
        #files_modified_time = spark.sql(f""" --select modificationTime from files_info_temp where name = '{last_processed_file}' """).collect()[0][0]
        files_modified_time = spark.sql(f"select  unix_timestamp(to_timestamp('{filter_prev_end_ts}'))").collect()[0][0]
    except:
        files_modified_time = '-2208988800' #spark.sql("select  unix_timestamp(to_timestamp('1900-01-01 00:00:00'))").collect()[0][0] Default timestamp value
    files_to_process = []
    files_to_process1 = spark.sql(f""" select collect_list(name) from files_info_temp where trim(modificationTime) > trim({files_modified_time}) """).collect()[0][0]
    
    files_to_process.extend(files_to_process1)
    files_processed = []
    if len(files_to_process) > 0:
        last_inserted_file = []
        for file in files_to_process:
            fixed_width_filepath_1 = f"{fixed_width_filepath}/{file}"
            prefix = file.split('_')[0]
            file_fixed_width_schema = fixed_width_schema["file_schema"][f"{prefix.lower()}"]
            process_path = lambda x: ('/'.join(x.split('/')[-4:]), x)
            file_name = process_path(fixed_width_filepath_1)[0]
            file_ts = str(str(fixed_width_filepath_1.split('/')[-1:]).split('.')[-2]).split('_')[-1]
            if var_file_encoding == 'windows-1252':
                text_df = spark.read.csv(fixed_width_filepath_1,sep = "\u0001", encoding="windows-1252")
                if (text_df.count() > 0):
                    text_df = text_df.toDF("value")    
            else:
                text_df = spark.read.text(fixed_width_filepath_1)
            
            if text_df.count() > 0:
                text_df = text_df.filter(~(col('value') == "")) 

            #print("var_skip_rows:",var_skip_rows,"\nfixed_width_filepath_1:",fixed_width_filepath_1)
            if (text_df.count() > 0) and var_skip_rows is not None:
                text_df = text_df.withColumn('index', monotonically_increasing_id())
                rows_to_remove = list(range(0,int(var_skip_rows))) 
                text_df = text_df.filter(~text_df.index.isin(rows_to_remove))
                text_df = text_df.drop('index')

            if text_df.count() > 0:
                df_src= text_df.select(*map(lambda x: text_df.value.substr(file_fixed_width_schema[x]['idx'], file_fixed_width_schema[x]['len']).alias(x), file_fixed_width_schema))
                #df_src = df_src.dropDuplicates()
                var_src_cnt=df_src.count()
                
                # Load file data into  temp view as a place holder 
                df_src.createOrReplaceTempView(v_tgt_tbl+'_tmp')
                var_tgt_tbl_cnt=df_src.count()
                if v_load_type.upper() == 'APPEND':
                    try:
                        v_sql_qry = f"""INSERT INTO {v_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS, '{file_name}' AS FILE_NAME, cast('{file_ts}' as string) AS FILE_TS
                                FROM {v_tgt_tbl}_tmp"""
                        spark.sql(v_sql_qry)
                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                        df_tgt_cnt = spark.sql(f"describe history {v_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} limit 1")
                        var_tgt_cnt = df_tgt_cnt.select('operationMetrics').collect()[0][0]['numOutputRows']
                        files_processed.append(file)
                        last_processed_file = file
                        spark.sql(f"UNCACHE TABLE IF EXISTS {v_tgt_tbl}_tmp")
                        spark.sql(f"DROP TABLE IF EXISTS {v_tgt_tbl}_tmp")
                    except Exception as e:
                        # print(e)
                        #UPDATE THE TASK CONTROL TABLE TO MARK THE FILE AS PROCESSED, once the file is loaded into the raw table , this elimates the duplicates in raw table incase of failure
                        # spark.sql(f"""  update {v_catalog_param}.{v_schema_nm_taskctrl}.{var_task_control}
                        #                 set last_processed_file = '{last_processed_file}' 
                        #                 where job_name = '{v_job_name}' and task_name = '{v_task_name}' and task_run_id = {v_task_run_id}
                        #         """)

                        spark.sql(f"DROP TABLE IF EXISTS {v_tgt_tbl}_tmp") 
                        spark.sql(f"DROP TABLE IF EXISTS files_list_temp") 
                        var_src_cnt, var_tgt_cnt, execution_end_time = 'NULL', 'NULL', datetime.datetime.now().replace(microsecond=0)
                        # last_processed_file = file
                        err_msgg = f'{last_processed_file}_err_msg_{str(e)}'
                        raise Exception(err_msgg)
            else:
                var_src_cnt, var_tgt_cnt, execution_end_time = 0, 0, datetime.datetime.now().replace(microsecond=0)
                last_processed_file = file
                files_processed.append(file)
    else:
        var_src_cnt, var_tgt_cnt, execution_end_time = 'NULL', 'NULL', datetime.datetime.now().replace(microsecond=0)

    spark.sql(f"DROP TABLE IF EXISTS files_info_temp")
    files_processed_1 =  f"{', '.join(files_processed)} files are processed" if len(files_processed) > 0 else 'No files available to process for the day'

    return var_src_cnt, var_tgt_cnt, execution_end_time , last_processed_file, files_processed_1

# COMMAND ----------

def fn_csv_file_ingestion(v_load_type,v_src_adls_path,v_src_extn,v_delim,v_is_hdr,v_tgt_schema,v_tgt_tbl,v_catalog_param, v_schema_nm_taskctrl, v_task_name, v_job_name, v_task_run_id,filter_prev_end_ts,var_skip_rows):
    ##########################################################################
    # Ingest CSV files from ADLS into target delta table
    #
    # Parameters:
    # v_load_type (str): The type of load operation (e.g., 'APPEND').
    # v_src_adls_path (str): The path to the source files in ADLS.
    # v_src_extn (str): The source file extension (e.g., 'csv').
    # v_delim (str): The delimiter used in the CSV files.
    # v_is_hdr (str): Indicates if the CSV files have a header ('true' or 'false').
    # v_tgt_schema (str): The target schema name.
    # v_tgt_tbl (str): The target table name.
    # v_catalog_param (str): The catalog parameter.
    # v_schema_nm_taskctrl (str): The schema name of the task control table.
    # v_task_name (str): The task name.
    # v_job_name (str): The job name.
    # v_task_run_id (str): The task run ID.
    # filter_prev_end_ts (str): The previous end timestamp for filtering files.
    # var_skip_rows (int): The number of rows to skip in the file.
    #
    # Returns:
    # tuple: A tuple containing the source count, target count, execution end time,
    #        last processed file, and a message indicating the files processed.
    ##########################################################################
    
    files_info = fn_get_file_info_from_adls(v_src_adls_path)
    files_info.createOrReplaceTempView("files_info_tmp")
    last_processed_file = fn_get_adls_last_processed_file(v_catalog_param, v_schema_nm_taskctrl,v_job_name,v_task_name)
    if last_processed_file is None or last_processed_file=='NULL':
        last_processed_file = re.sub("[',\],\[,']","",str(v_src_adls_path.split('/')[-1:]))+'_19000101000000.csv'
    if 'monthly' in v_src_adls_path:
        monthly_files_df = files_info.filter(col("path").contains("monthly"))
        # Get the latest file based on modification time
        latest_file = monthly_files_df.orderBy(col("modificationTime").desc()).limit(1).collect()[0].name
    try:
        files_modified_time = spark.sql(f"select  unix_timestamp(to_timestamp('{filter_prev_end_ts}'))").collect()[0][0]
    except:
        files_modified_time = '-2208988800' #spark.sql("select  unix_timestamp(to_timestamp('1900-01-01 00:00:00'))").collect()[0][0] Default timestamp value
    files_to_process = []
    files_to_process1 = spark.sql(f""" select collect_list(name) from files_info_tmp where trim(modificationTime) > trim({files_modified_time}) """).collect()[0][0]
    
    files_to_process.extend(files_to_process1)
    files_processed = []
    
    if len(files_to_process) > 0:
        last_inserted_file = []
        for file in files_to_process:
            v_src_adls_path_1 = f"{v_src_adls_path}/{file}"
            prefix = file.split('_')[0]
            # file_fixed_width_schema = fixed_width_schema["file_schema"][f"{prefix.lower()}"]
            process_path = lambda x: ('/'.join(x.split('/')[-4:]), x)
            file_name = process_path(v_src_adls_path_1)[0]
            file_ts = str(str(v_src_adls_path_1.split('/')[-1:]).split('.')[-2]).split('_')[-1]
            
            df_src = spark.read.format(f"{v_src_extn}")\
                                .option("header" ,f"{v_is_hdr}")\
                                .option("inferSchema", "False")\
                                .option("escape",'"')\
                                .option("ignoreLeadingWhiteSpace", "False")\
                                .option("ignoreTrailingWhiteSpace", "False")\
                                .load(v_src_adls_path,sep=f"{v_delim}")      
            
            if (df_src.count() > 0) and var_skip_rows is not None:
                df_src = df_src.withColumn('index', monotonically_increasing_id())
                rows_to_remove = list(range(0,int(var_skip_rows))) 
                df_src = df_src.filter(~df_src.index.isin(rows_to_remove))
                df_src = df_src.drop('index')
                

            # df_src.display()
            if df_src.count() > 0:
                var_src_cnt=df_src.count()
                
                # Load file data into  temp view as a place holder 
                df_src.createOrReplaceTempView(v_tgt_tbl+'_tmp')
                var_tgt_tbl_cnt=df_src.count()
                if v_load_type.upper() == 'APPEND':
                    try:
                        v_sql_qry = f"""INSERT INTO {v_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} 
                                SELECT  *,'{current_session_user}' AS LOAD_BY, CURRENT_TIMESTAMP() AS LOAD_TS, '{file_name}' AS FILE_NAME, cast('{file_ts}' as string) AS FILE_TS
                                FROM {v_tgt_tbl}_tmp"""
                        spark.sql(v_sql_qry)
                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                        df_tgt_cnt = spark.sql(f"describe history {v_catalog_param}.{v_tgt_schema}.{v_tgt_tbl} limit 1")
                        var_tgt_cnt = df_tgt_cnt.select('operationMetrics').collect()[0][0]['numOutputRows']
                        files_processed.append(file)
                        last_processed_file = file
                        spark.sql(f"UNCACHE TABLE IF EXISTS {v_tgt_tbl}_tmp")
                        spark.sql(f"DROP TABLE IF EXISTS {v_tgt_tbl}_tmp")
                    except Exception as e:
                        spark.sql(f"DROP TABLE IF EXISTS {v_tgt_tbl}_tmp") 
                        # spark.sql(f"DROP TABLE IF EXISTS files_list_tmp") 
                        var_src_cnt, var_tgt_cnt, execution_end_time = 'NULL', 'NULL', datetime.datetime.now().replace(microsecond=0)
                        # last_processed_file = file
                        err_msgg = f'{last_processed_file}_err_msg_{str(e)}'
                        raise Exception(err_msgg)
            else:
                var_src_cnt, var_tgt_cnt, execution_end_time = 0, 0, datetime.datetime.now().replace(microsecond=0)
                last_processed_file = file
                files_processed.append(file)
    else:
        var_src_cnt, var_tgt_cnt, execution_end_time = 'NULL', 'NULL', datetime.datetime.now().replace(microsecond=0)

    spark.sql(f"DROP TABLE IF EXISTS files_info_tmp")
    files_processed_1 =  f"{', '.join(files_processed)} files are processed" if len(files_processed) > 0 else 'No files available to process for the day'

    return var_src_cnt, var_tgt_cnt, execution_end_time , last_processed_file, files_processed_1

# COMMAND ----------

def update_task_control_restart(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status,last_processed_file='NULL'):
    ##########################################################################
    # Update the task control table with the restart information using parameter values. Raise exception if update fails with exception or reached the max retries incase of cuncurrent execution exception
    #
    # Parameters:
    # job_name (str): The name of the job.
    # job_id (str): The ID of the job.
    # parent_run_id (str): The parent run ID.
    # task_name (str): The name of the task.
    # task_run_id (str): The run ID of the task.
    # filter_start_time (str): The start timestamp for filtering.
    # filter_end_time (str): The end timestamp for filtering.
    # execution_start_time (str): The start timestamp for execution.
    # execution_end_time (str): The end timestamp for execution.
    # src_cnt (int): The source count.
    # tgt_cnt (int): The target count.
    # load_status (str): The status of the load operation.
    # last_processed_file (str): The last processed file (default is 'NULL').
    #
    # Returns:
    # None
    ##########################################################################
    query = f'''UPDATE {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control}
            SET task_run_id = "{task_run_id}", 
            filter_start_ts = to_timestamp("{filter_start_time}", "yyyy-MM-dd HH:mm:ss"),
            filter_end_ts = to_timestamp("{filter_end_time}", "yyyy-MM-dd HH:mm:ss"),
            execution_start_ts = to_timestamp("{execution_start_time}", "yyyy-MM-dd HH:mm:ss"),
            execution_end_ts = to_timestamp("{execution_end_time}", "yyyy-MM-dd HH:mm:ss"),
            source_count = {src_cnt},
            target_count = {tgt_cnt},
            status = "{load_status}",
            last_processed_file = "{last_processed_file}"
            WHERE task_name = "{task_name}"
            AND job_id = "{job_id}"
            AND parent_run_id = "{parent_run_id}"
            AND job_name = "{job_name}"
            AND upper(status) != "FAILED"  '''
    # spark.sql(query)
    retries = 0
    max_retries = 30
    initial_wait_time = 1
    backoff_factor = 2
    while retries < max_retries:
            try:
                    spark.sql(query)
                    break
            except ConcurrentAppendException as e:
                    retries += 1
                #     wait_time = initial_wait_time * (backoff_factor ** retries)
                    wait_time = random.randint(20, 45)
                    time.sleep(wait_time)
                    
    if retries == max_retries:
            raise Exception(f"Failed to update {var_task_control} after {max_retries} retries")
        

# COMMAND ----------

def update_task_control(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status):
        ###########################################################################
        # Update task control information using parameter values with last process file parameter. Raise exception if update fails with exception or reached the max retries incase of cuncurrent execution exception

        # Parameters:
        # job_name (str): The job name.
        # job_id (str): The job ID.
        # parent_run_id (str): The parent run ID.
        # task_name (str): The task name.
        # task_run_id (str): The task run ID.
        # filter_start_time (str): The filter start time.
        # filter_end_time (str): The filter end time.
        # execution_start_time (str): The execution start time.
        # execution_end_time (str): The execution end time.
        # src_cnt (int): The source count.
        # tgt_cnt (int): The target count.
        # load_status (str): The load status.
        ##########################################################################
        update_task_control_restart(job_name, job_id, parent_run_id, task_name, task_run_id, filter_start_time, filter_end_time, execution_start_time, execution_end_time, src_cnt, tgt_cnt, load_status)

# COMMAND ----------

def get_filter_start_ts(var_job_name,var_task_name):    
    ##########################################################################
    # Get the max filter end timestamp for a given job and task to use as filter start time for new run.

    # Parameters:
    # var_job_name (str): The name of the job.
    # var_task_name (str): The name of the task.
    #
    # Returns:
    # str: The filter end timestamp of the last completed task run, or a default
    #      timestamp ('1900-01-01 00:00:00') if no completed task run is found.
    ##########################################################################
    var_sql_query= """ select max(filter_end_ts) filter_end_ts from  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control+""" where job_name = '""" + var_job_name +"""' and task_name = '"""+var_task_name  +"""' and status ='COMPLETED'""" 
    #print(var_sql_query)    
    df_result1=spark.sql(var_sql_query)
    filter_prev_end_ts = df_result1.select('filter_end_ts').collect()[0][0]

    if filter_prev_end_ts is None:
            filter_prev_end_ts='1900-01-01 00:00:00'
    return filter_prev_end_ts

# COMMAND ----------

# Commented as it is not used in the code, Before uncommenting see if you shall use task_run_logging_sp_notebook if not please discuss with Murugan/Badri 
# def task_run_logging_recon(job_id, parent_run_id, task_run_id, procedure_name, create_dt, message_type, priority, message_cd, primary_message, secondary_message, login_id, data_key, records_in, records_out, procedure_runtime_stamp, records_updated):
#         ##########################################################################
#         # Log the task run details into the task run log table with the value for reconcilationm
#         #
#         # Parameters:
#         # job_id (str): The ID of the job.
#         # parent_run_id (str): The parent run ID.
#         # task_run_id (str): The run ID of the task.
#         # procedure_name (str): The name of the procedure.
#         # create_dt (str): The creation date.
#         # message_type (str): The type of the message.
#         # priority (int): The priority of the message.
#         # message_cd (str): The message code.
#         # primary_message (str): The primary message.
#         # secondary_message (str): The secondary message.
#         # login_id (str): The login ID.
#         # data_key (str): The data key.
#         # records_in (int): The number of records in.
#         # records_out (int): The number of records out.
#         # procedure_runtime_stamp (str): The runtime stamp of the procedure.
#         # records_updated (int): The number of records updated.
#         #
#         # Returns:
#         # None
#         ##########################################################################
#         #print(f"--------Logging started in {var_task_run_log} -----------")
#         query = f'''INSERT INTO {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_run_log}
#                     (job_id, parent_run_id, task_run_id, procedure_name, create_dt, message_type, priority, message_cd, primary_message, secondary_message, login_id, data_key, records_in, records_out, procedure_runtime_stamp, records_updated)
#                     VALUES ({job_id}, {parent_run_id}, {task_run_id},{procedure_name}, current_timestamp(),{message_type},{priority},{message_cd}, "{primary_message}","{secondary_message}",{login_id},{data_key},{src_cnt},{tgt_cnt},{procedure_runtime_stamp},{records_updated})'''



# COMMAND ----------

def fn_retry_query(v_query,max_retries:int = 3):
    ##########################################################################
    # Excute SQL query with retry in case of concurrent error.
    #
    # Parameters:
    # v_query (str): The SQL query to be executed.
    # max_retries (int): The maximum number of retry attempts. Default is 3.
    #
    # Returns:
    # None
    ##########################################################################
    for attempt in range(max_retries):
        attempt += 1
        try:
            spark.sql(v_query)
            break
        except Exception as excp:
            if 'concurrentappendexception' in str(excp).lower() or 'please try the operation again' in str(excp).lower(): # for Concurrent Update error
                time.sleep(10) #wait for 10 seconds
            else: #raise for other exceptions
                raise(excp)
        if attempt >= max_retries:
            raise Exception(f"Query failed after {attempt} retries")
    return 

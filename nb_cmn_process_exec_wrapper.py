# Databricks notebook source
# MAGIC %md
# MAGIC ###Common Wrapper Script
# MAGIC     - Driver notebooks for running the transformation pipeline with functionalities for invoking required transformation based on metadata. 
# MAGIC     - This is the notebook called in all history and incremental workflow. 
# MAGIC     - Based on  task_name or grouping_id parameter value passed from workflow task it retrive the metadata to trigger required action(Starging from ADLS->RAW, RAW-> Refined, Refined->Consume,Consume->Snowfalke).
# MAGIC
# MAGIC This script is designed to manage and execute various data processing tasks in a Databricks environment. 
# MAGIC It handles data ingestion, transformation, and loading across different layers (bronze, silver, gold) and 
# MAGIC supports integration with Snowflake and Synapse. The script also includes mechanisms for error handling, 
# MAGIC logging, and email notifications.
# MAGIC
# MAGIC Key Variables:
# MAGIC - var_env_nm: Environment name.
# MAGIC - var_job_name: Job name.
# MAGIC - var_workspace_instance_url: URL of the Databricks workspace instance.
# MAGIC - var_grouping_id: Grouping ID for tasks (optional - need to be send when task grouping is done for processing)
# MAGIC - var_run_dt: Run date.
# MAGIC - var_job_id: Job ID.
# MAGIC - var_parent_run_id: Parent run ID.
# MAGIC - var_task_run_id: Task run ID.
# MAGIC - var_task_name: Task name.
# MAGIC - var_job_strt_tms: Job start timestamp.
# MAGIC - var_job_status: Status of the job (1 for success, 0 for failure).
# MAGIC - var_recon_status: Reconciliation status (1 for success, 0 for failure).
# MAGIC
# MAGIC Main Steps:
# MAGIC 1. Initialize variables and widgets.
# MAGIC 2. Fetch current timestamp and set initial values for variables.
# MAGIC 3. Check for previous failed jobs and determine if the process should restart from the last failure point.
# MAGIC 4. Insert new log entries for failed jobs with 'NOT STARTED' status.
# MAGIC 5. Fetch the list of current tasks to be executed.
# MAGIC 6. Iterate through each task and execute based on the transformation type:
# MAGIC    - DML operations
# MAGIC    - Notebook execution
# MAGIC    - ADLS to Delta Lake ingestion
# MAGIC    - Table load operations
# MAGIC    - Data transfer to Snowflake
# MAGIC    - Data ingestion from Synapse to Delta Lake
# MAGIC    - File archival and housekeeping operations
# MAGIC 7. Update task control and log the status of each task.
# MAGIC 8. Handle exceptions and send email notifications on failure.
# MAGIC 9. Clean up temporary tables and finalize the process.
# MAGIC
# MAGIC Error Handling:
# MAGIC - Catches exceptions during task execution and updates the task control table with 'FAILED' status.
# MAGIC - Logs the error details and sends email notifications on failure.
# MAGIC - Cleans up temporary tables in case of exceptions.
# MAGIC
# MAGIC Email Notifications:
# MAGIC - Sends email notifications on task failure with relevant details.

# COMMAND ----------

###############################################################################
## Script Name    : nb_cmn_process_exec_wrapper                               #
## Creation Date  : 05-23-2024                                                #
## Created by     : Chubb LDM - Cognizant                                     #
## Purpose        : This is a reusable script is used to                      #
##                  1. Ingest data from adls to bronze/silver/gold layer      #
##                  2. Load data from synapse to raw layer                    #
##                  3. Transform data from bronze to silver, silver to gold   #
##                  4. Execute business logic notebooks                       #
##                  5. Load data to snowflake from gold layer                 #
##                  6. File archival                                          #
###############################################################################

# COMMAND ----------

# dbutils.widgets.text("ENV", "")
# dbutils.widgets.text("schema_nm_raw", "")
# dbutils.widgets.text("storage_account", "raw")
# # dbutils.widgets.text("process_nm", "wcs_fs")
# # # Reset restartability indicator flag, default will be : 0, for FULL load load value will be :1
# # dbutils.widgets.text(var_task_control"reset_restartability_ind", "0")
# # #dbutils.widgets.text("ins_tms", "2024-05-21 00:00:00.000")
# dbutils.widgets.text("RUN_DT", "")
# dbutils.widgets.text("JOB_ID", "1")
# dbutils.widgets.text("JOB_RUN_ID", "1234")
# dbutils.widgets.text("TASK_RUN_ID", "123456")
# # dbutils.widgets.text("file_format", "csv")

# dbutils.widgets.text("JOB_NM", "WF_ACERS_HISTORY")

# # dbutils.widgets.text("task_name", "T_WCS_ADLS_TO_BRONZE")

# dbutils.widgets.text("TASK_NM", "T_HISTORY_ADLS_TO_GOLD_1")

# dbutils.widgets.text("GROUPING_ID", "history_gd_grp1")

# COMMAND ----------

# MAGIC %run ./config/nb_cmn_env_config

# COMMAND ----------

# MAGIC %run ./config/nb_cmn_evn_snowflake

# COMMAND ----------

# MAGIC %run ./config/nb_cmn_evn_synapse

# COMMAND ----------

# MAGIC %run ./utils/nb_cmn_utils

# COMMAND ----------

var_env_nm = dbutils.widgets.get("ENV")
var_job_name = dbutils.widgets.get("JOB_NM")
var_workspace_instance_url =  dbutils.widgets.get("WORKSPACE_INSTANCE_URL")
try:
  var_grouping_id = dbutils.widgets.get("GROUPING_ID")
except Exception as e:
  var_grouping_id=None
  #print("var_grouping_id not received!")
var_run_dt = dbutils.widgets.get("RUN_DT")

var_job_id = dbutils.widgets.get("JOB_ID")
var_parent_run_id = dbutils.widgets.get("JOB_RUN_ID")
var_task_run_id = dbutils.widgets.get("TASK_RUN_ID") 

var_task_name = dbutils.widgets.get("TASK_NM")
var_job_strt_tms = dbutils.widgets.get("JOB_STRT_TMS")
#Table Names
# var_metadata_tbl = 'TBL_METADATA_CONFIG_DETAILS'
# var_task_control = 'TBL_TASK_CONTROL'
# var_run_run_log = 'TBL_TASK_RUN_LOG'
# var_ingest_all_files_from_adls='Y'
#to show job is success or failed 
var_job_status=1
var_recon_status=1

# COMMAND ----------

import datetime
from pyspark.sql.types import *
import sys
import re
from pyspark.errors import PySparkException
import time
import json

#print('Grouping id is : ', var_grouping_id)
load_status = 'COMPLETED'
# df_ins_ts=spark.sql("select current_date() as ins_tms")
# var_ins_tms= df_ins_ts.select('ins_tms').collect()[0][0]

var_ins_tms = datetime.datetime.now().strftime("%Y-%m-%d")
execution_start_time = datetime.datetime.now().replace(microsecond = 0)

#for back date file processing
var_is_backdated = 0

#assign 0 if running a failure task
var_pre_job_succes_status = 0

# if len(str(var_ins_tms))>=10:
#         var_ins_tms_dt=str(var_ins_tms)[:10]
#         #print(var_ins_tms_dt)
#         #var_ins_tms=fn_append_ts_todate(var_ins_tms_dt)
#         var_is_backdated=1

try:
        #check if process is triggered for restartability, to start from last failure point, default 0
        #print("Verify previous failed jobs!")
        history_df = spark.sql(f'DESCRIBE HISTORY {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control} LIMIT 1')
        version_no = history_df.collect()[0][0]
        #get recordshaving status other then success(2) or force completed(4)
        v_sql_qry = f"""SELECT * FROM (
                        SELECT job_name, job_id, parent_run_id, task_name, task_run_id,status,
                        row_number() over(order by execution_start_ts desc) as rn
                        FROM {var_catalog_param}.{var_schema_nm_taskctrl}.{var_task_control} version as of {version_no}
                        WHERE job_name = '{var_job_name}' AND parent_run_id = '""" + var_parent_run_id + """'
                        ) tmp """

        #print(v_sql_qry)
        df_result = spark.sql(v_sql_qry)
        df_result.createOrReplaceTempView(var_task_control+'_'+var_job_name+var_task_run_id+'_tmp')
        #print(df_result.count())
        # if df_result.count() > 0 : # BB Performance tuning - 02/12/24
        if df_result.isEmpty() == False:
                var_task_and_grouping_cond = ''
                #print("Job will be triggered from failure point! ")
                #var_pre_run_id = df_result.select('parent_run_id').collect()[0][0]
                if len(str(var_grouping_id)) > 0 :
                        #print("1=============")
                        var_task_and_grouping_cond = f" and  task_name in (select task_name from   {var_catalog_param}.{var_schema_nm_taskctrl}.{var_metadata_tbl} where grouping_id like '{var_grouping_id}' and is_active ='Y' )" 
                else:
                        #print("2=============")
                        var_task_and_grouping_cond = f" and  task_name like '{var_task_name}'" 

                #if var_pre_run_id == var_parent_run_id:
                #print(var_task_and_grouping_cond)
                #print(df_result.filter(f"parent_run_id like {var_parent_run_id} {var_task_and_grouping_cond} and status = 'FAILED'").count())

                sql_query = f"select * from  {var_task_control}_{var_job_name}{var_task_run_id}_tmp  where parent_run_id like '{var_parent_run_id}' {var_task_and_grouping_cond} and status = 'FAILED' "
                #print(sql_query)
                df_result_qry = spark.sql(sql_query)
                # if df_result_qry.count() > 0: # BB Performance tuning - 02/12/24
                if df_result_qry.isEmpty() == False:
                        #var_last_run_status =  df_result.select('status').collect()[0][0]
                        #if var_last_run_status =='FAILED':
                        if 'HISTORY' in var_job_name or len(str(var_grouping_id).strip()) > 0:
                                fltr_condition = f"""  job_name ='{var_job_name}' AND UPPER(is_active) = 'Y' 
                                AND grouping_id = '{var_grouping_id}' AND (job_name,TASK_NAME) in (select job_name,TASK_NAME from  """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp  as lg where parent_run_id = '""" + var_parent_run_id +"""' and task_name not in (select task_name from  """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp where status in ('INITIATED','NOT STARTED','COMPLETED')))"""
                        else:
                                fltr_condition ="""  job_name = '"""+ var_job_name+ """'  AND  UPPER         (is_active)   ='Y' and (job_name,TASK_NAME) in (select job_name,TASK_NAME from   """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp as lg where parent_run_id = '""" + var_parent_run_id +"""' and task_name not in (select task_name from  """+var_task_control+"""_"""+var_job_name+var_task_run_id+"""_tmp where status in ('INITIATED','NOT STARTED','COMPLETED')))"""

                        #print(fltr_condition)

                        #Insert new log for failed job with NOT STARTED status
                        var_sql_qry = """INSERT INTO  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control +""" (job_name ,job_id ,parent_run_id ,task_name ,status)
                                        SELECT         DISTINCT        
                                                job_name ,'"""+var_job_id+"""' ,'"""+var_parent_run_id+"""' ,task_name ,'NOT STARTED' 
                                FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +"""  WHERE """+  fltr_condition + var_task_and_grouping_cond

                        #assign 0 if running a failure task
                        var_pre_job_succes_status = 0

                else:
                        #print("If previous batch completed without failures1, so job will start from first step")
                        if 'HISTORY' in var_job_name  or len(str(var_grouping_id).strip()) > 0:
                                fltr_condition = f"""  job_name = '{var_job_name}' AND UPPER(is_active) = 'Y' 
                                AND grouping_id = '{var_grouping_id}'"""
                                #Insert  logs for all task with Not started status
                                var_sql_qry ="""INSERT INTO  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control +""" (job_name ,job_id ,parent_run_id ,task_name ,status)
                                                SELECT   distinct              
                                                        job_name ,'"""+var_job_id+"""' ,'"""+var_parent_run_id+"""' ,task_name ,'NOT STARTED' 
                                        FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +"""  WHERE """+  fltr_condition  
                                
                        else:
                                fltr_condition = """  job_name ='"""+ var_job_name+ """'  AND  UPPER(is_active) = 'Y'"""
                        
                                var_sql_qry = "select 1 "

        else:
                #print("If previous batch completed without failures, so job will start from first step")
                if 'HISTORY' in var_job_name or len(str(var_grouping_id).strip()) > 0:
                        fltr_condition = f"""  job_name ='{var_job_name}' AND UPPER(is_active) = 'Y' 
                        AND grouping_id = '{var_grouping_id}'"""
                else:
                        fltr_condition = """  job_name = '"""+ var_job_name+ """' AND    UPPER(is_active) = 'Y'  """
        
                #Insert  logs for all task with Not started status
                var_sql_qry = """INSERT INTO  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control +""" (job_name ,job_id ,parent_run_id ,task_name ,status)
                                SELECT   distinct              
                                        job_name ,'"""+var_job_id+"""' ,'"""+var_parent_run_id+"""' ,task_name ,'NOT STARTED' 
                        FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +"""  WHERE """+  fltr_condition                      
                                      
        #print(" Test 4 : Filter Criteria : ",fltr_condition)
        #Build query to fetch list of current tasks
        #print(f"var_grouping_id: {var_grouping_id}")

        if len(str(var_grouping_id).strip()) > 0:
                fnl_fltr_condition =  fltr_condition + """  AND  grouping_id = '"""+ var_grouping_id+ """' """       
        else:
                fnl_fltr_condition =  fltr_condition + """  AND  task_name = '"""+ var_task_name+ """' """
                
        var_sql_qry_1 = """ SELECT  distinct
                         'NOT STARTED' running_status_tracker,*
                                FROM """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+ var_metadata_tbl +""" 
                                where """+ fnl_fltr_condition 

        #print(var_sql_qry_1)
        df_result = spark.sql( var_sql_qry_1 )
        df_result.createOrReplaceTempView(var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp""")
        spark.sql("""create or replace  table """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp as select * from  """ +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp """)
        
        df_result = spark.sql("""select * from   """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp  """)
        #df_result.createOrReplaceTempView(var_metadata_tbl+'_'+var_job_name+'_tmp')
        #print('number of process in tmp : ',df_result.count())
        df_result_cnt = df_result.count()
        #print("Number of task to execute :", df_result_cnt)
        #print('-------Add entry in task control------------------',var_sql_qry)  
        #print('-------Add entry in task control------------------')  
        
        # df_result_qry = spark.sql(var_sql_qry)
        try:
                max_retries = 3
                fn_retry_query(var_sql_qry,int(max_retries))
        except Exception as e:
                raise(e)

        #df_result_qry_cnt = df_result_qry.count()
        #print(df_result_qry_cnt)

        var_sql_qry = ''
        
        if df_result_cnt > 0 :
                #print("inside task execution ")
                #df_result = spark.sql("""select * from  """ +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp""")
                #print(df_result.count())
                var_transformation_type = df_result.select('transformation_type').collect()[0][0]

                # if var_transformation_type.upper() in ('ADLS_TO_RAW','RAW_TO_REFINED','REFINED_TO_CONSUMED',
                #                                 'ADLS_TO_REFINED','ADLS_TO_CONSUMED'):
                #         #flg to count number of loads
                #         i = 0
                #         print(df_result.count())
                #         print("---------------------------")
                #         #src load logic inside for loop to handle  multiple jobs 

                #flg to count number of loads
                i=0
                #print("Number of process to execute : ", df_result.count())

                #src load logic inside for loop to handle  multiple jobs 
                #sys.exit()
                result_lst = df_result.toPandas().to_json(orient='records')
                for metadata in json.loads(result_lst):
                        #print(f'loading started for task - {i+1}')                              
                        var_src_cnt='0' 
                        var_tgt_tbl_cnt='0'
                        # Assign column values to variables
                        var_application = metadata['application']
                        var_job_nm = metadata['job_name']
                        var_task_name = metadata['task_name']
                        var_grouping_id = metadata['grouping_id']
                        var_transformation_type =metadata['transformation_type']
                        var_execute_child_notebook = metadata['execute_child_notebook']
                        var_load_type = metadata['load_type']
                        var_source_adls_path = metadata['source_adls_path']
                        var_source_file_extension = metadata['source_file_extension']
                        var_source_file_header = metadata['source_file_header']
                        var_source_file_delimiter = metadata['source_file_delimiter']
                        var_source_file_escape_quote = metadata['source_file_escape_quote']
                        var_source_schema = metadata['source_schema']
                        var_source_table = metadata['source_table']
                        var_target_notebook = metadata['target_notebook']
                        var_target_notebook_parameters = metadata['target_notebook_parameters']
                        var_target_schema = metadata['target_schema']
                        var_target_table = metadata['target_table']
                        var_merge_cols = metadata['merge_cols']
                        var_load_query = metadata['load_query']
                        var_archival_adls_path = metadata['archival_adls_path']
                        var_is_active = metadata['is_active']
                        var_ingest_all_files_from_adls = metadata['ingest_all_files_from_adls']
                        var_ingest_latest_files_from_adls = metadata['ingest_latest_files_from_adls']
                        var_ingest_oldest_files_from_adls = metadata['ingest_oldest_files_from_adls']
                        var_run_only_if_prev_job_success = metadata['run_only_if_prev_job_success']
                        var_skip_rows = metadata['skip_rows']
                        var_file_encoding = metadata['file_encoding']

                        if var_transformation_type is None:
                                #print("Transformation type is None:")
                                var_transformation_type = "None"

                        if var_load_type is None:
                                #print("Load type is None:")
                                var_load_type = "None"
                        
                        try:
                                if var_execute_child_notebook.strip().upper() == 'Y':
                                        var_execute_child_notebook='Y'
                                else:
                                        var_execute_child_notebook='N'
                        except:
                                var_execute_child_notebook='N'

                        #print("Transformation type is : ",var_transformation_type)
                        err_msg=''
                        var_is_archived=1

                        if var_transformation_type.upper() in ('DML_RAW','DML_REFINED','DML_CONSUMED','DML_OPERATION')  and var_load_type.upper() in ('DML')  and var_execute_child_notebook !='Y':
                                load_status = 'INITIATED' 
                                execution_start_time = datetime.datetime.now().replace(microsecond=0)
                                #print(execution_start_time)
                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name,var_task_run_id, '', '', execution_start_time, '', 'NULL', 'NULL', load_status)
                                try:
                                        #print(f'==================== DML QUERY  started ====================')
                                        dml_df = spark.sql(var_load_query)
                                        #dml_df.show()
                                        var_src_count = var_tgt_cnt =  dml_df.collect()[0][0]
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        #print(f'==================== DML Execution ended ====================')
                                        load_status = 'COMPLETED' 
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', f'INFO:{var_task_name} - {load_status}', var_src_count, var_tgt_cnt)
                                        # log Success task cntrl update 
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id,var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time,var_src_count, var_tgt_cnt, load_status)
                                except Exception as e:
                                        #print(e)
                                        load_status = 'FAILED'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        # log Failure task cntrl update 
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
                                        # log Failure aduit in run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e),'NULL', 'NULL')
                                        # print("Error Class       : " + type(e).__name__)
                                        # print("Message parameters: " + str(e.args))
                                        # print("SQLSTATE          : " + e.__class__.__name__)
                                        var_job_status=0
                                        # Send email notifications on failure

                                        var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name})
                                        #limiting error to 250 characters for sending in mail
                                        #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")                     

                        ## ============Code BLOCK TO EXECUTE THE NOTEBOOKS starts ============
                        if var_execute_child_notebook is not None and var_execute_child_notebook !='' and var_execute_child_notebook.upper() in ('Y'):
                                load_status = 'INITIATED' 
                                execution_start_time = datetime.datetime.now().replace(microsecond=0)
                                #print(execution_start_time)
                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name,var_task_run_id, '', '', execution_start_time, '', 'NULL', 'NULL', load_status)
                                if var_target_notebook is None:
                                        raise Exception("Notebook path cant be empty, if execute_child_notebook is Y")
                                        
                                else:
                                        try:
                                                filter_prev_end_ts = get_filter_start_ts(var_job_name,var_task_name)
                                                #print(f'==================== Notebook run started ====================')
                                                #print(f"Running notebook from {var_target_notebook}")
                                                #print(f"Notebook run parameters {var_target_notebook_parameters}")
                                                if var_target_notebook_parameters is None:
                                                        nb_run_details =  dbutils.notebook.run(f'{var_target_notebook}',0)
                                                else:
                                                        nb_run_details = dbutils.notebook.run(f'{var_target_notebook}', 0, eval(var_target_notebook_parameters))
                                                execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                                #print(f'==================== Notebook run ended ====================')
                                                #print(nb_run_details)
                                                nb_run_details = eval(nb_run_details)
                                                load_status = nb_run_details.get('task_run_status')
                                                task_run_msg = nb_run_details.get('task_run_msg')
                                                var_src_cnt = nb_run_details.get('src_cnt') #'NULL' if nb_run_details.get('src_cnt') == 0 else nb_run_details.get('src_cnt')
                                                var_tgt_cnt = nb_run_details.get('trgt_cnt') #'NULL' if nb_run_details.get('trgt_cnt') == 0 else nb_run_details.get('trgt_cnt
                                                if load_status == 'SUCCESS':
                                                        load_status = 'COMPLETED' 
                                                else:
                                                        load_status = 'FAILED'
                                                        raise Exception(f'AN error occured: {task_run_msg}')
                                                                                                
                                                task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', f'INFO:{var_task_name} - {load_status} - {task_run_msg}', var_src_cnt, var_tgt_cnt)
                                                # log Success task cntrl update 
                                                #Missing filter_start_ts & filter_end_ts variables for below taskcontrol function
                                                update_task_control(var_job_name, var_job_id, var_parent_run_id,var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_end_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status)
                                        except Exception as e:
                                                #print(e)
                                                load_status = 'FAILED'
                                                execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                                # log Failure task cntrl update 
                                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_end_time), execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
                                                # log Failure aduit in run log insert
                                                task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"',''),'NULL', 'NULL')
                                                # print("Error Class       : " + type(e).__name__)
                                                # print("Message parameters: " + str(e.args))
                                                # print("SQLSTATE          : " + e.__class__.__name__)
                                                var_job_status=0
                                                # Send email notifications on failure

                                                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name})
                                                #limiting error to 250 characters for sending in mail
                                                #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                         
                        ## ============Code BLOCK TO EXECUTE THE NOTEBOOKS ends ============
                        #if var_transformation_type.upper() in ('ADLS_TO_BRONZE','ADLS_TO_SILVER','ADLS_TO_GOLD'):
                        if var_transformation_type.upper() in ('ADLS_TO_RAW','ADLS_TO_REFINED','ADLS_TO_CONSUMED') and var_execute_child_notebook !='Y':                
                                # update as initiated in  task  cntrl 
                                load_status = 'INITIATED' 
                                execution_start_time = datetime.datetime.now().replace(microsecond=0)
                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, '', 'NULL', 'NULL', load_status)
                                #Get full path of file
                                var_file_path = var_storage_account+"""/"""+var_source_adls_path 
                                #print("""Source file ingestion started for """+ str(var_file_path))
                                #File header parameter 1 for True and others as False  
                                if var_source_file_header == 'Y':
                                        var_header='True'
                                else:
                                        var_header='False'
                                try:
                                        filter_prev_end_ts = get_filter_start_ts(var_job_name,var_task_name)
                                        try:
                                                var_ingest_all_files_from_adls.strip().upper() == 'Y'
                                        except:
                                                var_ingest_all_files_from_adls='N'
                                        if var_ingest_all_files_from_adls.strip().upper() == 'Y':
                                                
                                                var_src_cnt, var_tgt_cnt,execution_end_time ,last_processed_file, files_processed = fn_ingest_all_files_from_adls(var_file_path,var_text_files_schema, var_load_type, var_catalog_param, var_schema_nm_taskctrl ,var_target_schema, var_target_table, var_task_name, var_job_name, var_task_run_id,filter_prev_end_ts,var_skip_rows,var_file_encoding,var_source_file_extension)                                                
                                               
                                                
                                        #calling of function for file ingestion non fixed files
                                        else:
                                                var_file_path = get_latest_filename(var_file_path, '') if var_ingest_latest_files_from_adls == 'Y' else var_file_path
                                                var_src_cnt, var_tgt_cnt,execution_end_time = fn_src_tgt_ingestion_raw(var_load_type,var_file_path,var_source_file_extension,var_source_file_delimiter,var_header,var_target_schema,var_target_table)
                                        
                                        err_msg =''
                                        if str(var_src_cnt) != str(var_tgt_cnt) :
                                                #print("source and target counts are not matching! some issue with load process")
                                                load_status  = 'FAILED'
                                                err_msg = f'{var_task_name} - {load_status} source and target counts are not matching!'
                                                var_recon_status = 0
                                                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":"src vs trgt count not matching","TASK_NAME":var_task_name}) #limiting error to 30 characters for sending in mail
                                        else: 
                                                load_status = 'COMPLETED'
                                                err_msg = f'{var_task_name} - {load_status}' 
                                                
                                        # update status in task cntrl 
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        
                                        if var_ingest_all_files_from_adls.strip().upper() == 'Y':
                                                update_task_control_restart(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status,last_processed_file)
                                                err_msg = f'{err_msg},  {files_processed}'
                                        else:
                                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status)
                                        # log success aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'INFO:'+err_msg, var_src_cnt, var_tgt_cnt)
                                        err_msg =''

                                except Exception as e:
                                        load_status = 'FAILED'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        #  log Failure task cntrl update 
                                        if var_ingest_all_files_from_adls.strip().upper() == 'Y':
                                                last_processed_file =  str(e).split('_err_msg_')[0]
                                                # e = f'{var_task_name} - {load_status} {str(e).split("_err_msg_")[1]}'
                                                split_error = str(e).split("_err_msg_")
                                                # print("last_processed_file:",last_processed_file,"\nsplit_error",split_error)
                                                if len(split_error) >= 2:
                                                        e = f'{var_task_name} - {load_status} {split_error[1]}'
                                                else:
                                                        e = f'{var_task_name} - {load_status} {split_error[0]}'
                                                update_task_control_restart(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status,f'{last_processed_file}')
                                        else:
                                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
                                        # log failed aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"','').replace("'",""), 'NULL', 'NULL')
                                        #print("Error Class       : " + type(e).__name__)
                                        #print("Message parameters: " + str,'NULL', 'NULL'(e.args))
                                        #print("SQLSTATE          : " + e.__class__.__name__)
                                        var_job_status=0
                                        # Send email notifications on failure
                                        var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name}) #limiting error to 250 characters for sending in mail
                                        #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                                        if var_grouping_id != '' or len(var_grouping_id)>0:
                                                sys.exit()
                                        continue

                        #if var_transformation_type.upper() in ('BRONZE_TO_SILVER','BRONZE_TO_GOLD'):
                        if var_transformation_type.upper() in ('RAW_TO_REFINED','RAW_TO_CONSUMED','REFINED_TO_CONSUMED','REFINED_TO_REFINED','CONSUMED_TO_CONSUMED') and var_execute_child_notebook !='Y':                                
                                # update as initiated in   task  cntrl 
                                load_status = 'INITIATED' 
                                execution_start_time = datetime.datetime.now().replace(microsecond=0)
                                #print(execution_start_time)
                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, '', 'NULL', 'NULL', load_status) 
                                # Full load of tgt table using temp table (Source) 
                                try:
                                        # var_sql_query= """ select max(filter_end_ts) filter_end_ts from  """ + var_catalog_param + """."""+ var_schema_nm_taskctrl +"""."""+var_task_control+""" where job_name = '""" + var_job_name +"""' and task_name = '"""+var_task_name  +"""' and status ='COMPLETED'""" 
                                        # #print(var_sql_query)    
                                        # df_result1=spark.sql(var_sql_query )
                                        # filter_prev_end_ts = df_result1.select('filter_end_ts').collect()[0][0]
                                                                
                                        # if filter_prev_end_ts is None:
                                        #         filter_prev_end_ts='1900-01-01 00:00:00'

                                        filter_prev_end_ts = get_filter_start_ts(var_job_name,var_task_name)

                                        var_src_cnt, var_tgt_cnt = fn_table_load(v_load_type=var_load_type,v_src_schema=var_source_schema,v_src_tbl=var_source_table,v_tgt_schema=var_target_schema,v_tgt_tbl=var_target_table,v_load_query=var_load_query,v_merge_cols=var_merge_cols, v_filter_end_ts=filter_prev_end_ts)
                                        err_msg =''
                                        if str(var_src_cnt) != str(var_tgt_cnt) and (var_load_type.upper() not in ('UPSERT','MERGE_ONLY')):
                                                #print("source and target counts are not matching! some issue with load process")
                                                load_status  = 'FAILED'
                                                err_msg = f'{var_task_name} - {load_status} source and target counts are not matching!'
                                                var_recon_status = 0
                                                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":"src vs trgt count not matching","TASK_NAME":var_task_name}) #limiting error to 30 characters for sending in mail
                                        else:
                                                load_status = 'COMPLETED'
                                                err_msg = f'{var_task_name} - {load_status}'
                                        # update status in task cntrl                                          
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status)

                                        # log success aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL','INFO:'+err_msg, var_src_cnt, var_tgt_cnt)
                                        err_msg =''
                                                       
                                except Exception as e:
                                        load_status = 'FAILED'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                         # log Failure task cntrl update 
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
                                        # log failed aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"','').replace("'",""), 'NULL','NULL')
                                        # print(e)
                                        # print("Error Class       : " + type(e).__name__)
                                        # print("Message parameters: " + str(e.args))
                                        # print("SQLSTATE          : " + e.__class__.__name__)
                                        var_job_status=0
                                        # Send email notifications on failure
                                        var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name}) #limiting error to 250 characters for sending in mail
                                        #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                                        continue
             
                        #if var_transformation_type.upper() in ('GOLD_TO_SNOWFLAKE'):
                        if var_transformation_type.upper() in ('CONSUMED_TO_SNOWFLAKE','DML_SNOWFLAKE') and var_execute_child_notebook !='Y':
                                try:
                                        filter_prev_end_ts = None
                                        #call notebook/fn to copy src table to snowflake
                                        #print(var_transformation_type + " started !")
                                        # update as initiated in  task  cntrl 
                                        load_status = 'INITIATED' 
                                        execution_start_time = datetime.datetime.now().replace(microsecond = 0)
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, '', 'NULL', 'NULL', load_status)                                        
                                        var_src_cnt, var_tgt_cnt = fn_write_deltalake_to_snowflake(v_load_type = var_load_type, v_src_schema = var_source_schema, v_src_tbl = var_source_table, v_tgt_schema = var_target_schema, v_tgt_tbl = var_target_table, v_load_query = var_load_query, v_merge_cols = var_merge_cols, v_filter_end_ts = None)
                                        err_msg =''
                                        if str(var_src_cnt) != str(var_tgt_cnt) and var_transformation_type.upper() in ('CONSUMED_TO_SNOWFLAKE'):                                       
                                                #print("source and target counts are not matching! some issue with load process")
                                                load_status  = 'FAILED'
                                                err_msg = f'{var_task_name} - {load_status} source and target counts are not matching!'
                                                var_recon_status = 0
                                                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":"src vs trgt count not matching","TASK_NAME":var_task_name}) #limiting error to 30 characters for sending in mail
                                        else: 
                                                load_status = 'COMPLETED'
                                                err_msg = f'{var_task_name} - {load_status}'
                                        # update status in task cntrl                                          
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status)
                                        # log success aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', f'INFO:{var_task_name} - {load_status}', var_src_cnt, var_tgt_cnt)
                                        err_msg =''

                                except Exception as e:
                                        load_status = 'FAILED'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                         # log Failure task cntrl update 
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', '', execution_end_time, 'NULL', 'NULL', load_status)
                                        # log failed aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"','').replace("'",""),'NULL', 'NULL')
                                        #print(e)
                                        # print("Error Class       : " + type(e).__name__)
                                        # print("Message parameters: " + str(e.args))
                                        # print("SQLSTATE          : " + e.__class__.__name__)
                                        var_job_status=0
                                        # Send email notifications on failure
                                        var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name}) #limiting error to 250 characters for sending in mail
                                        #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                                        continue

                        #FOR LOADING DATA FROM THE SYNAPSE TO DELTA LAKE
                        if var_transformation_type.upper() in ('SYNAPSE_TO_RAW') and var_execute_child_notebook !='Y':                                
                                # update as initiated in task cntrl 
                                filter_prev_end_ts = None
                                load_status = 'INITIATED' 
                                execution_start_time = datetime.datetime.now().replace(microsecond=0)
                                #print(execution_start_time)
                                update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, '', 'NULL', 'NULL', load_status) 
                                try:
                                        ########### CALLING SYNAPSE FUNCTION ######
                                        var_src_cnt, var_tgt_cnt = fn_src_tgt_ingestion_synapse_to_deltalake(v_load_type=var_load_type,v_src_schema=var_source_schema,v_src_tbl=var_source_table,v_tgt_schema=var_target_schema,v_tgt_tbl=var_target_table,v_load_query=var_load_query,v_merge_cols=var_merge_cols)
                                        err_msg=''
                                        if str(var_src_cnt) != str(var_tgt_cnt) and (var_load_type.upper() !='UPSERT' or var_load_type.upper() !='MERGE_ONLY'):
                                                #print("source and target counts are not matching! some issue with load process")
                                                load_status  = 'FAILED'
                                                err_msg = f'{var_task_name} - {load_status} source and target counts are not matching!'
                                                var_recon_status = 0
                                                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":"src vs trgt count not matching","TASK_NAME":var_task_name}) #limiting error to 30 characters for sending in mail
                                        else: 
                                                load_status = 'COMPLETED'
                                                err_msg = f'{var_task_name} - {load_status}'
                                        # update status in task cntrl                                          
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status)
                                        # log success aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'INFO:'+err_msg, var_src_cnt, var_tgt_cnt)
                                        err_msg=''

                                except Exception as e:
                                        load_status = 'FAILED'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                         # log Failure task cntrl update 
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
                                        # log failed aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e),'NULL', 'NULL')
                                        # print(e)
                                        # print("Error Class       : " + type(e).__name__)
                                        # print("Message parameters: " + str(e.args))
                                        # print("SQLSTATE          : " + e.__class__.__name__)
                                        var_job_status=0
                                        # Send email notifications on failure
                                        var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name}) #limiting error to 250 characters for sending in mail
                                        # print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                                        continue 

                        if var_transformation_type.upper() in ('ADLS_FILE_ARCHIVE','ADLS_FOLDER_ARCHIVE','ADLS_FILE_HOUSEKEEPING') and var_execute_child_notebook !='Y':
                                try:
                                        filter_prev_end_ts = None
                                        #call notebook/fn to copy src table to snowflake
                                        #print(var_transformation_type + " started !")
                                        # update as initiated in  task  cntrl 
                                        load_status = 'INITIATED' 
                                        execution_start_time = datetime.datetime.now().replace(microsecond = 0)
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, '', 'NULL', 'NULL', load_status)
                                                                             
                                        #var_src_cnt, var_tgt_cnt = fn_write_deltalake_to_snowflake(v_load_type = var_load_type, v_src_schema = var_source_schema, v_src_tbl = var_source_table, v_tgt_schema = var_target_schema, v_tgt_tbl = var_target_table, v_load_query = var_load_query, v_merge_cols = var_merge_cols, v_filter_end_ts = None)
                                        if var_transformation_type.upper() in ('ADLS_FILE_ARCHIVE'):
                                                var_src_cnt, var_tgt_cnt=fn_arch_file(v_file_path=var_storage_account+"""/"""+var_source_adls_path ,v_archival_adls_path=var_storage_account+"""/"""+var_archival_adls_path)
                                        elif var_transformation_type.upper() in ('ADLS_FOLDER_ARCHIVE'):
                                                var_src_cnt, var_tgt_cnt=fn_arch_folder(v_folder_path=var_storage_account+"""/"""+var_source_adls_path,v_archival_adls_folder_path=var_storage_account+"""/"""+var_archival_adls_path)
                                        else  :
                                                var_src_cnt, var_tgt_cnt=fn_housekeeping_files(v_folder_path=var_storage_account+"""/"""+var_source_adls_path)
                                        err_msg=''
                                        if str(var_src_cnt) != str(var_tgt_cnt) and var_transformation_type.upper() in ('ADLS_FILE_ARCHIVE','ADLS_FOLDER_ARCHIVE'):
                                                #print("source and target counts are not matching! some issue with load process")
                                                load_status  = 'FAILED'
                                                err_msg = f'{var_task_name} - {load_status} source and target counts are not matching!'
                                                var_recon_status = 0
                                                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":"src vs trgt count not matching","TASK_NAME":var_task_name}) #limiting error to 30 characters for sending in mail
                                        else: 
                                                load_status = 'COMPLETED'
                                                err_msg = f'{var_task_name} - {load_status}'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0) 
                                        # update status in task cntrl
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, str(filter_prev_end_ts), str(execution_start_time), execution_start_time, execution_end_time, var_src_cnt, var_tgt_cnt, load_status)

                                        # log success aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'INFO:'+err_msg, var_src_cnt, var_tgt_cnt)
                                        err_msg=''

                                except Exception as e:
                                        load_status = 'FAILED'
                                        execution_end_time = datetime.datetime.now().replace(microsecond=0)
                                         # log Failure task cntrl update 
                                        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', '', execution_end_time, 'NULL', 'NULL', load_status)
                                        # log failed aduit in  run log insert
                                        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"',''),'NULL', 'NULL')
                                        # print(e)
                                        # print("Error Class       : " + type(e).__name__)
                                        # print("Message parameters: " + str(e.args))
                                        # print("SQLSTATE          : " + e.__class__.__name__)
                                        var_job_status=0
                                        # Send email notifications on failure
                                        var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name}) #limiting error to 250 characters for sending in mail
                                        #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                                        continue

                        #print(f'loading ended for the task - {i+1}')   
                        #print('---------------------------------------------------------')
                        #Increment index for next file ingestion in case of multiple source files
                        i = i+1;
                #drop temp table on completion!
                spark.sql("""drop table if exists    """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp """)

                if i == 0:
                        #print("No task data found for the given source name.")
                        pass
                if var_job_status == 0 or var_recon_status == 0:
                        # Send email notifications on failure
                        # var_send_email_nb_run_details = dbutils.notebook.run(var_nb_email_notifications_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed" ,"JOB_RUN_ID": var_parent_run_id,"ERROR_MESSAGE":str(e)[:250]}) #limiting error to 250 characters for sending in mail
                        # print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                        #print("some jobs are in failed status, Please check log.")
                        sys.exit()

except Exception as e:
        #print(e)
        #drop temp table if exception occured!
        spark.sql("""drop table if exists    """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp """)
        #logging for job failure 
        load_status = 'FAILED'
        execution_end_time = datetime.datetime.now().replace(microsecond=0)
        # log Failure task cntrl update 
        time.sleep(10)
        update_task_control(var_job_name, var_job_id, var_parent_run_id, var_task_name, var_task_run_id, '', '', execution_start_time, execution_end_time, 'NULL', 'NULL', load_status)
        # log Failure aduit in run log insert
        task_run_logging(var_job_id, var_parent_run_id, 'NULL', var_task_run_id, 'NULL', 'CRITICAL:'+str(e).replace('"',''),'NULL', 'NULL')

        if var_job_status == 0 or var_recon_status == 0:
                #print("some jobs are in failed status!")
                # Send email notifications on failure
                var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name})
                #limiting error to 250 characters for sending in mail
                #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
                sys.exit()
        var_send_email_nb_run_details = dbutils.notebook.run(var_email_notification_nb_path, 0, {"SUB_APP":var_str_subapp_nm.upper(), "NOTIFICATION_TYPE": "failed", "JOB_RUN_ID": var_parent_run_id,"ENV":var_str_env_nm, "WORKSPACE_INSTANCE_URL":var_workspace_instance_url, "CATALOG_NAME":var_catalog_param, "TASK_CONTROL_TBL": var_schema_nm_taskctrl, "ERROR_MESSAGE":str(e)[:30],"TASK_NAME":var_task_name})
        #limiting error to 250 characters for sending in mail
        #print(f"Email Notification sent on failure :\n {var_send_email_nb_run_details}")
        raise(e)
finally:
        #print('Finally Block')
        spark.sql("""drop table if exists    """ + var_catalog_param + """."""+ var_schema_nm_rfnd +""".""" +var_metadata_tbl+"""_"""+var_job_name+ str(var_task_run_id)+"""_tmp """)        

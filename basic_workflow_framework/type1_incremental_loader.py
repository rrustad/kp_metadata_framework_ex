# Databricks notebook source
from pyspark.sql import SparkSession,functions as F
from delta.tables import DeltaTable
from pyspark.sql.functions import concat, current_date, col, to_timestamp, expr, sha2, concat_ws, current_timestamp, lit, to_date
from pyspark.sql.types import *
from pyspark.sql import *
from delta import *
import json
import sys
from datetime import datetime, timedelta

# COMMAND ----------

dbutils.widgets.text("catalog_name", "default_catalog_name", "Catalog Name")
dbutils.widgets.text("environment_name", "default_environment_name", "Environment Name")
dbutils.widgets.text("tbl_name", "default_table_name", "Table Name")

# COMMAND ----------

def read_catalog_config(config_file_path):
    """
    Read catalog configuration from a JSON file.
    """
    with open(config_file_path, "r") as file:
        config = json.load(file)
        return config
    
def get_environment_params(config, catalog_name, environment_name):
    """
    Get environment-specific parameters from the catalog configuration.
    """
    return config["catalogs"][catalog_name]["environments"].get(environment_name, {})
    

def convert_table_columns_to_dict(lst):
    result_dict = {}
    for i in range(0, len(lst)):
        result_dict[lst[i]] = lst[i]
        table_columns_dict = {f"TGT.{key}" : f"STG.{val}" for key, val in result_dict.items()}
    return table_columns_dict


def get_audit_cols_dict():
    aud_cols_insert_values_dict = {
                            "TGT.AUD_ROW_ID": "1", 
                            "TGT.AUD_END_TS": """to_date('4000-12-31', 'yyyy-MM-dd')""",
                            "TGT.AUD_UPDATE_TS": "current_timestamp",
                            "TGT.AUD_ACTIVE_ROW_CODE":"1"
                            }
    return aud_cols_insert_values_dict


def get_target_table_instance(target_table_path):
    targetTable = DeltaTable.forName(spark,target_table_path )
    return targetTable

    
def get_audit_del_dict():
    aud_cols_insert_values_dict = {
                            "TGT.AUD_ROW_ID": "2", 
                            "TGT.AUD_END_TS": """to_date('4000-12-31', 'yyyy-MM-dd')""",
                            "TGT.AUD_UPDATE_TS": "current_timestamp"
                            }
    return aud_cols_insert_values_dict
    

def get_source_data(spark, source_table_path, primary_keys,from_load_dt, to_load_dt):                
   src_df = spark.sql(f"""
                        select   STG.*, replace(concat({primary_keys}),'.0','') pk_cols, RANK() over (partition by {primary_keys}
                        ORDER BY AUD_OPERATION_TIME DESC) as RANK  from 
                               {source_table_path} as STG WHERE  (date(AUD_LOAD_DT) > date('{from_load_dt}') and date(AUD_LOAD_DT) <= date('{to_load_dt}'))  AND AUD_OPERATION_OWNER='NULL';               
                        
                     """)
   return src_df


def run_incremental_load(spark, catalog_name, environment_name, tbl_name,params):
    source_schema = params.get("staging_schema")
    target_schema = params.get("enriched_schema")
    audit_log_table = params.get("audit_log_table")
    table_list = params.get("table_list")
    table_meta = params.get("incr_table_metadata", None)
    region_code = params.get("region_code")
    table_meta=table_meta

    # Debugging information
    print(f"Catalog Name: {catalog_name}")
    print(f"Source Schema: {params.get('staging_schema', '')}")
    print(f"Table Meta: {table_meta}")
    
    dt = datetime.now()
    current_time= dt.strftime('%Y%m%d%H%M')

    if table_meta:
        qry = f"""SELECT table_id, table_name, primary_keys, join_conditions, 
                    date(last_processed_date), dateadd(DAY,1,date(last_processed_date))
                    FROM {catalog_name}.{source_schema}.{table_meta} where table_name = '{tbl_name}'"""
        meta_table_df = spark.sql(qry)
    
        incr_load_list = [list (row) for row in meta_table_df.collect()]
        for index, param_value in enumerate(incr_load_list):
            table_id = param_value[0]
            table_name = param_value[1]
            primary_keys = param_value[2]
            join_conditions = param_value[3]
            from_load_dt = param_value[4]
            to_load_dt = param_value[5]
            
            print(table_name)
            print(f"from dt = {from_load_dt}, to date = {to_load_dt}")
            print(type(from_load_dt))
            print(type(to_load_dt))

            source_table_path =  f"{catalog_name}.{source_schema}.{table_name}"
            target_table_path =  f"{catalog_name}.{target_schema}.{table_name}"
            audit_table_path =  f"{catalog_name}.{source_schema}.{audit_log_table}"
            delete_table_path= f"{catalog_name}.{source_schema}.del_{table_name}"
            
            id =str(current_time)+'-'+str(table_id)

            try:

                get_type1_data(spark, source_table_path, target_table_path, primary_keys, join_conditions, audit_table_path, id,delete_table_path,table_name,region_code,from_load_dt, to_load_dt)

                update_processed_dt =f"""update {catalog_name}.{source_schema}.{table_meta} 
                        set last_processed_date=date('{to_load_dt}') where table_name='{tbl_name}';"""
                spark.sql(update_processed_dt)
            
            except Exception as e:
                # Log the error and continue to the next iteration
                print(f"Error: {str(e)}")

                with open("error_log.txt", "a") as log_file:
                    log_file.write(f"Error: {str(e)}\n")
                continue

# COMMAND ----------

def get_type1_data(spark, source_table_path, target_table_path, primary_keys, join_conditions, audit_table_path, id,delete_table_path,table_name,region_code,from_load_dt, to_load_dt):
    
    df_src_1 = get_source_data(spark, source_table_path, primary_keys,from_load_dt, to_load_dt)
    df_total_count=df_src_1.count()
    print(f"total_count = {df_total_count}")
    df_src=df_src_1.filter(df_src_1.RANK==1)
    df_src_rank1_count = df_src.count()
    print(f"rank1 total_count = {df_src_rank1_count}")

    #df_src.createOrReplaceTempView("temp_stg_view")
    df_src_rank = df_src.drop("AUD_TRANSACTION_ID", "AUD_OPERATION_TYPE","AUD_OPERATION_SEQUENCE","RANK")
    
    deletes_count=0
    total_count_duplicates=0
    df_src_rank=df_src_rank.distinct()
    
    delete_df = spark.sql(f"select distinct replace(replace(pk_cols,',',''),'.0','') as pk_cols  from {delete_table_path} where  (delivered_date > date('{from_load_dt}') and delivered_date <= date('{to_load_dt}'))")

    df_dup = (delete_df.join(df_src_rank,delete_df.pk_cols == df_src_rank.pk_cols, "left_outer")
            .where(df_src_rank.pk_cols.isNull())
            .where(to_date(df_src_rank.AUD_LOAD_DT)> from_load_dt)
            .where(to_date(df_src_rank.AUD_LOAD_DT) <= to_load_dt)
            .select(delete_df.pk_cols)
            )

    deletes_count=df_dup.count()
    total_count=df_total_count+deletes_count
    
    targetTable = get_target_table_instance(target_table_path)
    targetTable.alias("TGT").merge(
                source =  df_dup.alias("STG"), 
                condition = f"replace(concat({primary_keys}),'.0','') = replace(STG.pk_cols,'.0','')")\
                        .whenMatchedUpdate(set = {"TGT.AUD_ACTIVE_ROW_CODE" : '2',
                                                  "TGT.AUD_END_TS" : "to_date('4000-12-31', 'yyyy-MM-dd')",
                                                  "TGT.AUD_UPDATE_TS" : current_timestamp()})\
                                                          .execute()

    
    df_src_rank=df_src_rank.drop("pk_cols","AUD_LOAD_DT","AUD_OPERATION_OWNER")
    df_src_rank=df_src_rank.withColumnRenamed("AUD_OPERATION_TIME","AUD_CREATE_TS")
    df_src_rank=df_src_rank.distinct()

    table_columns_dict = convert_table_columns_to_dict(df_src_rank.columns)
    audit_columns_dict = get_audit_cols_dict()
    insert_dict=table_columns_dict|audit_columns_dict

    print("df_src_rank before merge")
    df_src_rank.display()

    try:
        targetTable.alias("TGT").merge(
                source =  df_src_rank.alias("STG"), 
                condition = f"{join_conditions}"
                    
                ).whenMatchedUpdate(set = insert_dict
                ).whenNotMatchedInsert(values = insert_dict
                ).execute()
          
        qry_process_ind=f"update {source_table_path} set AUD_OPERATION_OWNER='Y' WHERE (date(AUD_LOAD_DT) > date('{from_load_dt}') and date(AUD_LOAD_DT) <= date('{to_load_dt}'))" 
        spark.sql(qry_process_ind) 

        qry_insert_update_count=f"""select  operationMetrics.numTargetRowsInserted as inserted, operationMetrics.numTargetRowsUpdated as updated, operationMetrics.executionTimeMs
        from (describe history {target_table_path} limit 1)
        where operation = 'MERGE'""" 
        df_insert_update=spark.sql(qry_insert_update_count)

        inserts_count=int(df_insert_update.collect()[0][0])
        updates_count=int(df_insert_update.collect()[0][1])
        total_time_taken_ms = int(df_insert_update.collect()[0][2])

        start_time=datetime.now() - timedelta( milliseconds=total_time_taken_ms)

        total_count_duplicates=total_count-(inserts_count+updates_count+deletes_count)
         
        qry = f"insert into {audit_table_path} (id,src_table,tgt_table,source_count,target_count,load_type,load_status,start_time,end_time,inserted,updated,deleted,duplicate_records,total_records,status_description,app_name,region_code,AUD_TIME) values('{id}','{table_name}','{table_name}',0,0,'Dbx-Type1','Success','{start_time}',current_timestamp(),{inserts_count},{updates_count},{deletes_count},{total_count_duplicates},{total_count},'Success','NULL','{region_code}','{to_load_dt}')"
        spark.sql(qry)

    except Exception as e:
            qry = f"insert into {audit_table_path} (id,src_table,tgt_table,source_count,target_count,load_type,load_status,start_time,end_time,inserted,updated,deleted,duplicate_records,total_records,status_description,app_name,region_code,AUD_TIME) values('{id}','{table_name}','{table_name}',0,0,'Dbx-Type1','Failed',current_timestamp(),current_timestamp(),0,0,{deletes_count},{total_count_duplicates},{total_count},'{e}','NULL','{region_code}','{to_load_dt}')"
            spark.sql(qry)
              
              
              
def main(catalog_name, environment_name, tbl_name):
    # Read the JSON catalog configuration file
    config_file_path = "../python/config/clarity_integration.json"
    catalog_config = read_catalog_config(config_file_path)
    
    try:
        # Get parameters for the selected environment
        environment_params = get_environment_params(catalog_config, catalog_name, environment_name)
        
    except KeyError as e:
        print(f"Error: {e}. Please check if the provided catalog and environment names are correct.")
        sys.exit(1)

    # Initialize Spark session
    spark = SparkSession.builder.appName("Incremental_Load").getOrCreate()


    # Run the incremental load for the selected environment
    run_incremental_load(spark, catalog_name, environment_name, tbl_name, params=environment_params)

# COMMAND ----------

if __name__ == "__main__":
    main(dbutils.widgets.get("catalog_name"), dbutils.widgets.get("environment_name"), dbutils.widgets.get("tbl_name"))

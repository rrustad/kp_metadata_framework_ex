# Databricks notebook source
import pyspark.sql.functions as f
from pyspark.sql.window import Window
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text('source_table_path', '')
source_table_path = dbutils.widgets.get('source_table_path')
dbutils.widgets.text('target_table_path', '')
target_table_path = dbutils.widgets.get('target_table_path')
dbutils.widgets.text('primary_keys', '')
primary_keys = dbutils.widgets.get('primary_keys')
dbutils.widgets.text('join_conditions', '')
join_conditions = dbutils.widgets.get('join_conditions')
dbutils.widgets.text('audit_table_path', '')
audit_table_path = dbutils.widgets.get('audit_table_path')
dbutils.widgets.text('id', '')
id = dbutils.widgets.get('id')
dbutils.widgets.text('delete_table_path', '')
delete_table_path = dbutils.widgets.get('delete_table_path')
dbutils.widgets.text('table_name', '')
table_name = dbutils.widgets.get('table_name')
dbutils.widgets.text('region_code', '')
region_code = dbutils.widgets.get('region_code')
dbutils.widgets.text('from_load_dt', '')
from_load_dt = dbutils.widgets.get('from_load_dt')
dbutils.widgets.text('to_load_dt', '')
to_load_dt = dbutils.widgets.get('to_load_dt')
dbutils.widgets.text('ext_loc', '')
ext_loc = dbutils.widgets.get('ext_loc')

if ',' in primary_keys:
  pks = primary_keys.split(',')
else:
  pks = [primary_keys]

# COMMAND ----------

def table_exists(table_path):
  catalog, schema, table = table_path.split('.')
  df = (
    spark.table(f'{catalog}.information_schema.tables')
    .filter(
      (f.col('table_schema')==schema) &
      (f.col('table_name')==table)
      )
  )
  return df.count() > 0

# COMMAND ----------

# dbutils.fs.rm(ext_loc, True)
# spark.sql(f"drop table if exists {source_table_path+'_updates'}")
# spark.sql(f"drop table if exists {target_table_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create table kp_catalog.clarity_enr.audit

# COMMAND ----------

def get_source_data(spark, source_table_path, pks):

   if ',' in primary_keys:
      pks = primary_keys.split(',')
   else:
      pks = [primary_keys]

   return (
      spark.readStream.format('delta')
      .table(source_table_path)
      .withColumns({pk:f.regexp_replace(pk,'.0','') for pk in pks})
      .filter(f.col('AUD_OPERATION_OWNER')=='NULL')
      )

# COMMAND ----------

def get_delete_data(spark, delete_table_path, pks):
   


   return (
      spark.readStream.format('delta')
      .table(delete_table_path)
      .withColumns({pk:f.regexp_replace(pk,'.0','') for pk in pks})
      .withColumn('op', f.lit('delete'))
      )

# COMMAND ----------

df_src_1 = get_source_data(spark, source_table_path, pks)

# COMMAND ----------

# df_total_count=df_src_1.count()
# print(f"total_count = {df_total_count}")
df_src=df_src_1#.filter(df_src_1.RANK==1)
# df_src_rank1_count = df_src.count()
# print(f"rank1 total_count = {df_src_rank1_count}")

#df_src.createOrReplaceTempView("temp_stg_view")
df_src_rank = df_src.drop("AUD_TRANSACTION_ID", "AUD_OPERATION_TYPE","AUD_OPERATION_SEQUENCE","RANK")

deletes_count=0
total_count_duplicates=0
# Do we need this? This is super expensive
# df_src_rank=df_src_rank.distinct()

delete_df = get_delete_data(spark, delete_table_path, pks)
# delete_df = spark.sql(f"select distinct replace(replace(pk_cols,',',''),'.0','') as pk_cols  from {delete_table_path} where  (delivered_date > date('{from_load_dt}') and delivered_date <= date('{to_load_dt}'))")

# COMMAND ----------

(
  df_src_rank
  .writeStream
  .option("checkpointLocation", ext_loc+source_table_path.split('.')[-1])
  .trigger(availableNow=True)
  .toTable(source_table_path+'_updates')
  .awaitTermination()
)

# COMMAND ----------

(
  delete_df
  .writeStream
  .option("checkpointLocation", ext_loc+delete_table_path.split('.')[-1])
  .trigger(availableNow=True)
  .toTable(source_table_path+'_updates')
  .awaitTermination()
)

# COMMAND ----------

(
  spark.table(source_table_path+'_updates')
  .withColumn('rnk', f.dense_rank().over(
    Window.partitionBy("id").orderBy(
      f.col("AUD_OPERATION_TIME").desc(), 
      # I don't think you need the below line, If you have the same AUD_operation_time it'll dupe
      f.col('op')
      )
    ))
  .filter(f.col('rnk') == 1)
  .drop('rnk')
  .withColumnRenamed("AUD_OPERATION_TIME","AUD_CREATE_TS")
  .withColumn("AUD_ROW_ID",  f.lit("1"))
  .withColumn("AUD_END_TS",  f.to_date(f.lit('4000-12-31'), 'yyyy-MM-dd'))
  .withColumn("AUD_UPDATE_TS",  f.current_timestamp())
  .withColumn("AUD_OPERATION_OWNER",  f.lit('Y'))
  # .withColumn("AUD_ACTIVE_ROW_CODE", f.lit("1"))
).createOrReplaceTempView('updates')

# COMMAND ----------

if not table_exists(target_table_path):
  # you guys shouldn't need to do this - you have a script that creates these tables for you
  spark.sql(f"create or replace table {target_table_path} as select * except (op) from updates")
  spark.sql(f"truncate table {target_table_path}")
  spark.sql(f"alter table {target_table_path} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


# COMMAND ----------

df = spark.sql(f"""
MERGE INTO kp_catalog.clarity_enr.table1 TGT
USING updates AS STG
ON {' AND '.join([f"STG.{pk}==TGT.{pk}" for pk in pks])}
WHEN MATCHED AND STG.op = 'delete' THEN DELETE
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED AND STG.op != 'delete' THEN INSERT *

""")

# COMMAND ----------

spark.sql(f"truncate table {source_table_path+'_updates'}")

# COMMAND ----------

spark.table(target_table_path).display()

# COMMAND ----------

qry_insert_update_count=f"""select  operationMetrics.numTargetRowsInserted as inserted, operationMetrics.numTargetRowsUpdated as updated, operationMetrics.executionTimeMs
from (describe history {target_table_path} limit 1)
where operation = 'MERGE'""" 
df_insert_update=spark.sql(qry_insert_update_count)

inserts_count=int(df_insert_update.collect()[0][0])
updates_count=int(df_insert_update.collect()[0][1])
total_time_taken_ms = int(df_insert_update.collect()[0][2])

#This doesn't make sense to recreate the time
# start_time=datetime.now() - timedelta( milliseconds=total_time_taken_ms)

total_count_duplicates=total_count-(inserts_count+updates_count+deletes_count)
  
qry = f"insert into {audit_table_path} (id,src_table,tgt_table,source_count,target_count,load_type,load_status,start_time,end_time,inserted,updated,deleted,duplicate_records,total_records,status_description,app_name,region_code,AUD_TIME) values('{id}','{table_name}','{table_name}',0,0,'Dbx-Type1','Success','{start_time}',current_timestamp(),{inserts_count},{updates_count},{deletes_count},{total_count_duplicates},{total_count},'Success','NULL','{region_code}','{to_load_dt}')"
spark.sql(qry)

# COMMAND ----------



# COMMAND ----------

df_dup = (delete_df.join(df_src_rank,delete_df.pk_cols == df_src_rank.pk_cols, "left_outer")
        .where(df_src_rank.pk_cols.isNull())
        .where(to_date(df_src_rank.AUD_LOAD_DT)> from_load_dt)
        .where(to_date(df_src_rank.AUD_LOAD_DT) <= to_load_dt)
        .select(delete_df.pk_cols)
        )


# Counts processed rows - delete, we can get this from
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

# COMMAND ----------



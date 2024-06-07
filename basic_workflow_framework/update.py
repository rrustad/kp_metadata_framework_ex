# Databricks notebook source
import pyspark.sql.functions as f
from datetime import datetime, timedelta

# COMMAND ----------


dbutils.widgets.text('source_catalog', 'kp_catalog')
source_catalog = dbutils.widgets.get('source_catalog')

dbutils.widgets.text('source_schema', 'stg_clarity')
source_schema = dbutils.widgets.get('source_schema')

# COMMAND ----------

def update_table(source_catalog, source_schema, table_name, pk):
  df = spark.table(f"{source_catalog}.{source_schema}.{table_name}")
  max_pk = df.select(f.max(pk)).collect()[0][0]
  
  max_dt = df.select(f.max('AUD_LOAD_DT')).collect()[0][0]
  max_dt_plus_one = (max_dt+timedelta(days=1)).strftime('%Y-%m-%d')
  
  foo = df.orderBy(f.rand()).limit(1).select('foo').collect()[0][0]

  delete_pk = df.orderBy(f.rand()).limit(1).select(pk).collect()[0][0]

  spark.sql(f"insert into {source_catalog}.{source_schema}.{table_name} ({pk}, foo, op, AUD_LOAD_DT, AUD_OPERATION_TIME, AUD_OPERATION_OWNER) values ({max_pk}, '{foo}.1', 'update', '{max_dt_plus_one}', '{max_dt_plus_one}', 'NULL')") 
  spark.sql(f"insert into {source_catalog}.{source_schema}.{table_name} ({pk}, foo, op, AUD_LOAD_DT, AUD_OPERATION_TIME, AUD_OPERATION_OWNER) values ({max_pk + 1}, 'foo{max_pk + 1}', 'insert', '{max_dt_plus_one}', '{max_dt_plus_one}', 'NULL')")
  spark.sql(f"insert into {source_catalog}.{source_schema}.{table_name}_del ({pk}, AUD_LOAD_DT, AUD_OPERATION_TIME, AUD_OPERATION_OWNER) values ({delete_pk}, '{max_dt_plus_one}', '{max_dt_plus_one}', 'NULL')")
  print(f"update {source_catalog}.clarity_stg.table_status SET status_ready = 'Y' WHERE source_table = '{table_name}'")
  spark.sql(f"update {source_catalog}.clarity_stg.table_status SET status_ready = 'Y' WHERE source_table = '{table_name}'")

# COMMAND ----------

update_table(source_catalog, source_schema, 'table1', 'tb1pk1')
update_table(source_catalog, source_schema, 'table2', 'tb2pk1')
update_table(source_catalog, source_schema, 'table3', 'tb3pk1')
update_table(source_catalog, source_schema, 'table4', 'tb4pk1')
update_table(source_catalog, source_schema, 'table5', 'tb5pk1')

# COMMAND ----------

table_name = 'table1'
spark.sql(f"update {source_catalog}.clarity_stg.table_status SET status_ready = 'Y' WHERE source_table = '{table_name}'").display()

# COMMAND ----------



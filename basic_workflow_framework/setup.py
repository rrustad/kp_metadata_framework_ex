# Databricks notebook source
import pyspark.sql.functions as f
from datetime import datetime, timedelta

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists kp_catalog;
# MAGIC create schema if not exists kp_catalog.clarity_stg;
# MAGIC create schema if not exists kp_catalog.clarity_enr;

# COMMAND ----------

(spark.createDataFrame([
  [1, 'foo1', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [2, 'foo2', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [3, 'foo3', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
], schema="id integer, foo string, op string, AUD_LOAD_DT timestamp, AUD_OPERATION_TIME timestamp, AUD_OPERATION_OWNER string")
.write
.mode('overwrite')
.saveAsTable('kp_catalog.clarity_stg.table1')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table kp_catalog.clarity_stg.table1_del
# MAGIC (
# MAGIC   id integer,
# MAGIC   AUD_LOAD_DT timestamp,
# MAGIC   AUD_OPERATION_TIME timestamp,
# MAGIC   AUD_OPERATION_OWNER string
# MAGIC   )

# COMMAND ----------

def update_table(table_name, pk):
  df = spark.table(table_name)
  max_pk = df.select(f.max('id')).collect()[0][0]
  
  max_dt = df.select(f.max('AUD_LOAD_DT')).collect()[0][0]
  max_dt_plus_one = (max_dt+timedelta(days=1)).strftime('%Y-%m-%d')
  
  foo = df.orderBy(f.rand()).limit(1).select('foo').collect()[0][0]

  delete_pk = df.orderBy(f.rand()).limit(1).select('id').collect()[0][0]

  spark.sql(f"insert into {table_name} (id, foo, op, AUD_LOAD_DT, AUD_OPERATION_TIME, AUD_OPERATION_OWNER) values ({max_pk}, '{foo}.1', 'update', '{max_dt_plus_one}', '{max_dt_plus_one}', 'NULL')") 
  spark.sql(f"insert into {table_name} (id, foo, op, AUD_LOAD_DT, AUD_OPERATION_TIME, AUD_OPERATION_OWNER) values ({max_pk + 1}, 'foo{max_pk + 1}', 'insert', '{max_dt_plus_one}', '{max_dt_plus_one}', 'NULL')")
  spark.sql(f"insert into {table_name}_del (id, AUD_LOAD_DT, AUD_OPERATION_TIME, AUD_OPERATION_OWNER) values ({delete_pk}, '{max_dt_plus_one}', '{max_dt_plus_one}', 'NULL')") 


update_table('kp_catalog.clarity_stg.table1', 'id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kp_catalog.clarity_stg.table1

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kp_catalog.clarity_stg.table1_del

# COMMAND ----------

# %sql
# drop schema if exists kp_catalog.clarity_stg cascade;
# drop schema if exists kp_catalog.clarity_enr cascade;

# COMMAND ----------



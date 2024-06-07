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
], schema="tb1pk1 integer, foo string, op string, AUD_LOAD_DT timestamp, AUD_OPERATION_TIME timestamp, AUD_OPERATION_OWNER string")
.write
.mode('overwrite')
.saveAsTable('kp_catalog.clarity_stg.table1')
 )

(spark.createDataFrame([
  [1, 1, 'foo1', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [2, 2, 'foo2', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [3, 3, 'foo3', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
], schema="tb2pk1 integer, tb2pk2 integer, foo string, op string, AUD_LOAD_DT timestamp, AUD_OPERATION_TIME timestamp, AUD_OPERATION_OWNER string")
.write
.mode('overwrite')
.saveAsTable('kp_catalog.clarity_stg.table2')
 )

(spark.createDataFrame([
  [1, 1, 'foo1', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [2, 2, 'foo2', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [3, 3, 'foo3', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
], schema="tb3pk1 integer, tb3pk2 integer, foo string, op string, AUD_LOAD_DT timestamp, AUD_OPERATION_TIME timestamp, AUD_OPERATION_OWNER string")
.write
.mode('overwrite')
.saveAsTable('kp_catalog.clarity_stg.table3')
 )

(spark.createDataFrame([
  [1, 'foo1', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [2, 'foo2', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [3, 'foo3', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
], schema="tb4pk1 integer, foo string, op string, AUD_LOAD_DT timestamp, AUD_OPERATION_TIME timestamp, AUD_OPERATION_OWNER string")
.write
.mode('overwrite')
.saveAsTable('kp_catalog.clarity_stg.table4')
 )

(spark.createDataFrame([
  [1,1,1, 'foo1', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [2,2,2, 'foo2', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
  [3,3,3, 'foo3', 'insert', datetime(year=2000, month=1, day=1),datetime(year=2000, month=1, day=1), 'NULL'],
], schema="tb5pk1 integer,tb5pk2 integer,tb5pk3 integer, foo string, op string, AUD_LOAD_DT timestamp, AUD_OPERATION_TIME timestamp, AUD_OPERATION_OWNER string")
.write
.mode('overwrite')
.saveAsTable('kp_catalog.clarity_stg.table5')
 )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from kp_catalog.clarity_stg.table1

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table kp_catalog.clarity_stg.table1_del
# MAGIC (
# MAGIC   tb1pk1 integer,
# MAGIC   AUD_LOAD_DT timestamp,
# MAGIC   AUD_OPERATION_TIME timestamp,
# MAGIC   AUD_OPERATION_OWNER string
# MAGIC   );
# MAGIC
# MAGIC create or replace table kp_catalog.clarity_stg.table2_del
# MAGIC (
# MAGIC   tb2pk1 integer,
# MAGIC   tb2pk2 integer,
# MAGIC   AUD_LOAD_DT timestamp,
# MAGIC   AUD_OPERATION_TIME timestamp,
# MAGIC   AUD_OPERATION_OWNER string
# MAGIC   );
# MAGIC create or replace table kp_catalog.clarity_stg.table3_del
# MAGIC (
# MAGIC   tb3pk1 integer,
# MAGIC   tb3pk2 integer,
# MAGIC   AUD_LOAD_DT timestamp,
# MAGIC   AUD_OPERATION_TIME timestamp,
# MAGIC   AUD_OPERATION_OWNER string
# MAGIC   );
# MAGIC create or replace table kp_catalog.clarity_stg.table4_del
# MAGIC (
# MAGIC   tb4pk1 integer,
# MAGIC   AUD_LOAD_DT timestamp,
# MAGIC   AUD_OPERATION_TIME timestamp,
# MAGIC   AUD_OPERATION_OWNER string
# MAGIC   );
# MAGIC create or replace table kp_catalog.clarity_stg.table5_del
# MAGIC (
# MAGIC   tb5pk1 integer,
# MAGIC   tb5pk2 integer,
# MAGIC   tb5pk3 integer,
# MAGIC   AUD_LOAD_DT timestamp,
# MAGIC   AUD_OPERATION_TIME timestamp,
# MAGIC   AUD_OPERATION_OWNER string
# MAGIC   )

# COMMAND ----------

# %sql
# drop schema if exists kp_catalog.clarity_stg cascade;
# drop schema if exists kp_catalog.clarity_enr cascade;

# COMMAND ----------



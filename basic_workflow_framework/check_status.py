# Databricks notebook source
import time
import pyspark.sql.functions as f

# COMMAND ----------

dbutils.widgets.text('source_catalog', 'kp_catalog')
source_catalog = dbutils.widgets.get('source_catalog')

dbutils.widgets.text('source_table', 'table1')
source_table = dbutils.widgets.get('source_table')

# COMMAND ----------

status = 'N'

while status == 'N':
  s = (
    spark.table(f'{source_catalog}.clarity_stg.table_status')
    .filter(f.col('source_table') == source_table)
  ).collect()
  assert len(s) == 1
  status = s[0]['status_ready']
  time.sleep(10)

# COMMAND ----------



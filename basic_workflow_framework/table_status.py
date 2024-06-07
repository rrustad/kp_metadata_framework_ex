# Databricks notebook source
dbutils.widgets.dropdown('init', 'False', ['True', 'False'])
init = dbutils.widgets.get('init') == 'True'
dbutils.widgets.text('source_catalog', 'kp_catalog')
source_catalog = dbutils.widgets.get('source_catalog')

# COMMAND ----------

if init:
  spark.sql(f"""create or replace table {source_catalog}.clarity_stg.table_status (
    source_schema STRING, 
    source_table STRING, 
    status_ready STRING)
    """)
  df = spark.createDataFrame([
    ["stg_clarity","table1",'N'],
    ["stg_clarity","table2",'N'],
    ["stg_clarity","table3",'N'],
    ["stg_clarity","table4",'N'],
    ["stg_clarity","table5",'N'],
  ], schema="source_schema string, source_table string, status_ready string")
  df.write.mode('append').saveAsTable(f'{source_catalog}.clarity_stg.table_status')
  spark.sql(f"ALTER TABLE {source_catalog}.clarity_stg.table_status SET TBLPROPERTIES ('delta.enableDeletionVectors' = true)")

# COMMAND ----------

spark.table(f'{source_catalog}.clarity_stg.table_status').display()

# COMMAND ----------



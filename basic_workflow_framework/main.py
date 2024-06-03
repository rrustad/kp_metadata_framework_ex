# Databricks notebook source
dbutils.widgets.text('source', 'table1')
dbutils.widgets.text('target', 'table1_')

# COMMAND ----------

source = dbutils.widgets.get('source')
target = dbutils.widgets.get('target')

# COMMAND ----------

(
  spark.table(source)
  .write
  .mode('overwrite')
  .saveAsTable(target)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into control table "{source} update"

# Databricks notebook source
# MAGIC %run ./_includes

# COMMAND ----------

for i in range(10):
  (spark.createDataFrame([
    [1, 'foo1'],
    [2, 'foo2'],
    [3, 'foo3'],
  ], schema=f'id_{i} integer, foo string')
   .write.mode('overwrite').saveAsTable(f'table{i}')
  )

# COMMAND ----------



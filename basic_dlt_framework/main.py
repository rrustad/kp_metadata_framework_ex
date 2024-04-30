# Databricks notebook source
import dlt
from pyspark.sql.functions import *
import json

# COMMAND ----------

table_map = spark.conf.get("table_map")
print(table_map)

# COMMAND ----------

table_map = json.loads(table_map)

# COMMAND ----------

def create_table(source, target):
  @dlt.table(name=target)
  def t():
    return spark.read.table(f"kp_catalog.metadata_framework.{source}")

for source, target in table_map.items():
  create_table(source, target)

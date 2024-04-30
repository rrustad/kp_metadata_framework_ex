# Databricks notebook source
import json

# COMMAND ----------

def define_pipeline(batches, batch, table, table_map):
    return {
        "pipeline_type": "WORKSPACE",
        "clusters": [
            {
                "label": "default",
                "autoscale": {
                    "min_workers": 1,
                    "max_workers": 5,
                    "mode": "ENHANCED"
                }
            },
            {
                "label": "maintenance"
            }
        ],
        "development": True,
        "continuous": False,
        "channel": "CURRENT",
        "photon": True,
        "libraries": [
            {
                "notebook": {
                    "path": "/Repos/riley.rustad@databricks.com/kp_metadata_framework_ex/basic_dlt_framework/main"
                }
            }
        ],
        "name": f"metadata_framework_batch{batch}",
        "edition": "ADVANCED",
        "catalog": "kp_catalog",
        "configuration": {
            "table_map": json.dumps(table_map)
        },
        "target": "metadata_framework",
        "data_sampling": False
    }

# COMMAND ----------

with open('./metadata.json','r') as f:
  batches = json.load(f)
batches

# COMMAND ----------

for batch in batches:
  print(batch)
  table_map = {}
  for table in batches[batch]['tables']:
    print(table)
    table_map[table] = batches[batch]['tables'][table]['target']
  pipeline_def = define_pipeline(batches, batch, table, table_map)
  with open(f"./pipelines/batch{batch}.json","w") as f:
    json.dump(pipeline_def, f, indent=2)

# COMMAND ----------

# import requests
# url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
# token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


# _token = {
#             'Authorization': 'Bearer {0}'.format(token),
#         }
# r = requests.post(url + '/api/2.0/pipelines', headers=_token, data = json.dumps(pipeline_def))
# r.json()

# COMMAND ----------



# Databricks notebook source
import json

# COMMAND ----------

with open('./metadata.json','r') as f:
  batches = json.load(f)
batches

# COMMAND ----------


for batch in batches:
  tasks = []
  for table in batches[batch]['tables']:
    tasks.append({
      "task_key": f"{table}",
      "run_if": "ALL_SUCCESS",
      "notebook_task": {
        "notebook_path": "/Repos/riley.rustad@databricks.com/kp_metadata_framework_ex/main",
        "base_parameters": {
          "source": f"{table}",
          "target": f"{table}_"
        },
        "source": "WORKSPACE"
      },
      "job_cluster_key": f"batch{batch}_cluster"
    })

  cluster = batches[batch]['cluster']
  job_def = {
    "name": f"metadata_framework_job_{batch}",
    "tasks":tasks,
    "job_clusters": [
      {
        "job_cluster_key": f"batch{batch}_cluster",
        "new_cluster": {
          "spark_version": cluster['spark_version'],
          "instance_pool_id": cluster['instance_pool_id'],
          "data_security_mode": cluster['data_security_mode'],
          "num_workers": cluster['num_workers']
        }
      }
    ]
  }
  
  with open(f"./workflows/batch{batch}.json","w") as f:
    json.dump(job_def, f, indent=2)

# COMMAND ----------

import requests
url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None) 
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)


_token = {
            'Authorization': 'Bearer {0}'.format(token),
        }
r = requests.post(url + '/api/2.1/jobs/create', headers=_token, data = json.dumps(job_def))
r.json()

# COMMAND ----------

# TODO: Implement job update

# COMMAND ----------

# TODO: Implement job delete

# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/Users/anupammishrajsp@gmail.com/DatabricksRepo/Backfill_POC"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# COMMAND ----------

dbutils.widgets.text("mode", "INITIAL")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("workflow_run_id", "")
dbutils.widgets.text("request_id", "")

# COMMAND ----------

mode = dbutils.widgets.get("mode")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
workflow_run_id = dbutils.widgets.get("workflow_run_id")
request_id = dbutils.widgets.get("request_id")

# COMMAND ----------

if not workflow_run_id:
    raise ValueError(
        "workflow_run_id is required for Gold."
    )

# COMMAND ----------

from src.transformation.gold_transformation_module import (
    GoldTransformation
)

# COMMAND ----------

from src.transformation.gold_transformation_module import (
    GoldTransformation
)

gold_job = GoldTransformation(spark)

result = gold_job.run(
    mode=mode,
    run_id=workflow_run_id,
    request_id=request_id or None,
    start_date=start_date or None,
    end_date=end_date or None
)

print(result)
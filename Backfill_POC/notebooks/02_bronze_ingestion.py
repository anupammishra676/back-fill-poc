# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/Users/anupammishrajsp@gmail.com/DatabricksRepo/Backfill_POC"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

print("Project path configured.")

# COMMAND ----------

dbutils.widgets.text("mode", "INITIAL")
dbutils.widgets.text("start_date", "")
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("request_id", "")

# COMMAND ----------

mode = dbutils.widgets.get("mode")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
request_id = dbutils.widgets.get("request_id")

# COMMAND ----------

from src.config.config_module import CONFIG
from src.ingestion.bronze_ingestion_module import BronzeIngestion
from src.utils.spark_utils_module import SparkUtils

# COMMAND ----------

workflow_run_id = SparkUtils.generate_run_id("WF")

print(f"Workflow Run ID: {workflow_run_id}")

dbutils.jobs.taskValues.set(
    key="workflow_run_id",
    value=workflow_run_id
)

# COMMAND ----------

bronze_job = BronzeIngestion(spark)

result = bronze_job.run(
    mode=mode,
    run_id=workflow_run_id,
    request_id=request_id or None,
    start_date=start_date or None,
    end_date=end_date or None
)

print(result)
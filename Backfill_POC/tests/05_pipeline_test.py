# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/Users/anupammishrajsp@gmail.com/DatabricksRepo/Backfill_POC"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# COMMAND ----------

from src.utils.spark_utils_module import SparkUtils

from src.ingestion.bronze_ingestion_module import BronzeIngestion
from src.transformation.silver_transformation_module import SilverTransformation
from src.transformation.gold_transformation_module import GoldTransformation

# COMMAND ----------

run_id = SparkUtils.generate_run_id("WF")

print(f"Workflow Run ID: {run_id}")

# COMMAND ----------

bronze_job = BronzeIngestion(spark)

bronze_result = bronze_job.run(
    mode="INITIAL",
    run_id=run_id
)

print(bronze_result)

# COMMAND ----------

silver_job = SilverTransformation(spark)

silver_result = silver_job.run(
    mode="INITIAL",
    run_id=run_id
)

print(silver_result)

# COMMAND ----------

gold_job = GoldTransformation(spark)

gold_result = gold_job.run(
    mode="INITIAL",
    run_id=run_id
)

print(gold_result)
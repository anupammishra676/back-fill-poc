# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/Users/anupammishrajsp@gmail.com/DatabricksRepo/Backfill_POC"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

print("Project root added to Python path.")

# COMMAND ----------

from src.config.config import CONFIG
from src.utils.logging_utils import ApplicationLogger
from src.utils.spark_utils import SparkUtils
from src.audit.audit_logger import AuditLogger

print("All framework modules imported successfully.")

# COMMAND ----------

logger = ApplicationLogger.get_logger("FrameworkSetup")

logger.info("Starting framework setup validation.")

run_id = SparkUtils.generate_run_id("SETUP")

logger.info(f"Generated run ID: {run_id}")

audit_logger = AuditLogger(spark)

logger.info("Framework setup validation completed.")
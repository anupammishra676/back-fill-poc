# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

import sys

PROJECT_ROOT = "/Workspace/Users/anupammishrajsp@gmail.com/DatabricksRepo/Backfill_POC"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

print("Project path configured.")

# COMMAND ----------

from src.backfill.backfill_service_module import BackfillService

# COMMAND ----------

api_key = dbutils.secrets.get(
    scope="openai",
    key="api_key"
)

# COMMAND ----------

service = BackfillService(
    spark=spark,
    api_key=api_key
)

# COMMAND ----------

request_text = """
The HR source data for 2026-08-10 through
2026-08-12 was corrected. Please reprocess the affected data.
"""

request = service.create_request_from_text(
    request_text=request_text,
    requested_by="demo_user"
)

print(request.request_id)

# COMMAND ----------

service.reject_request(
    request_id=request.request_id,
    rejected_by="business_owner",
    rejection_reason="Business owner does not want this historical correction processed at this time."
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     request_id,
# MAGIC     status,
# MAGIC     approval_required,
# MAGIC     rejected_by,
# MAGIC     rejected_at,
# MAGIC     rejection_reason
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC WHERE request_id = 'REQ-20260830074500-056F1C';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     request_id,
# MAGIC     requested_by,
# MAGIC     request_text,
# MAGIC     business_unit,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     status,
# MAGIC     risk_level,
# MAGIC     impact_summary,
# MAGIC     proposed_strategy,
# MAGIC     approval_required,
# MAGIC     approved_by,
# MAGIC     approved_at,
# MAGIC     created_at,
# MAGIC     updated_at
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC ORDER BY created_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC WHERE request_id = 'REQ-20260830070525-2AA105';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS total_requests,
# MAGIC     MAX(created_at) AS latest_created_at
# MAGIC FROM data_engineering.backfill_poc.backfill_request;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC WHERE request_text LIKE '%2026-08-25%'
# MAGIC   AND request_text LIKE '%2026-08-20%'
# MAGIC ORDER BY created_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     request_id,
# MAGIC     business_unit,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     status,
# MAGIC     risk_level,
# MAGIC     approval_required,
# MAGIC     approved_by,
# MAGIC     approved_at,
# MAGIC     rejected_by,
# MAGIC     rejected_at,
# MAGIC     rejection_reason,
# MAGIC     created_at,
# MAGIC     updated_at
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC ORDER BY COALESCE(updated_at, created_at) DESC;

# COMMAND ----------

request_id = "REQ-20260830132658-E09287"

from src.backfill.backfill_execution_module import (
    BackfillExecutionService
)

execution_service = BackfillExecutionService(spark)

execution = execution_service.prepare_execution(
    request_id=request_id
)

print(execution)

# COMMAND ----------

request_text = """
The Finance source data for 2026-08-20 through
2026-08-22 was corrected. Please reprocess the affected data.
"""

request = service.create_request_from_text(
    request_text=request_text,
    requested_by="demo_user"
)

print(request)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     request_id,
# MAGIC     business_unit,
# MAGIC     start_date,
# MAGIC     end_date,
# MAGIC     status,
# MAGIC     risk_level,
# MAGIC     approval_required,
# MAGIC     approved_by,
# MAGIC     approved_at,
# MAGIC     created_at,
# MAGIC     updated_at
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC ORDER BY created_at DESC;

# COMMAND ----------

request_id = "REQ-20260831150439-A861D4"

service.approve_request(
    request_id=request_id,
    approved_by="business_owner"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC *
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC WHERE request_id = 'REQ-20260831150439-A861D4';

# COMMAND ----------

# DBTITLE 1,Execute backfill for approved request
request_id = "REQ-20260831150439-A861D4"

from src.backfill.backfill_execution_module import (
    BackfillExecutionService
)

execution_service = BackfillExecutionService(spark)

result = execution_service.execute_backfill(
    request_id=request_id
)

print(result)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     request_id,
# MAGIC     layer,
# MAGIC     operation,
# MAGIC     status,
# MAGIC     records_read,
# MAGIC     records_written,
# MAGIC     processing_start_date,
# MAGIC     processing_end_date,
# MAGIC     created_at
# MAGIC FROM data_engineering.backfill_poc.pipeline_run_log
# MAGIC WHERE request_id = 'REQ-20260831150439-A861D4'
# MAGIC ORDER BY created_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     br.request_id,
# MAGIC     br.status AS backfill_status,
# MAGIC     br.execution_id,
# MAGIC     COUNT(pl.request_id) AS audit_records
# MAGIC FROM data_engineering.backfill_poc.backfill_request br
# MAGIC LEFT JOIN data_engineering.backfill_poc.pipeline_run_log pl
# MAGIC     ON br.request_id = pl.request_id
# MAGIC WHERE br.request_id = 'REQ-20260831150439-A861D4'
# MAGIC GROUP BY
# MAGIC     br.request_id,
# MAGIC     br.status,
# MAGIC     br.execution_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     request_id,
# MAGIC     status,
# MAGIC     execution_id,
# MAGIC     execution_started_at,
# MAGIC     execution_completed_at,
# MAGIC     execution_error
# MAGIC FROM data_engineering.backfill_poc.backfill_request
# MAGIC WHERE request_id = 'REQ-20260831150439-A861D4';

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     request_id,
# MAGIC     layer,
# MAGIC     status,
# MAGIC     error_message
# MAGIC FROM data_engineering.backfill_poc.pipeline_run_log
# MAGIC WHERE request_id = 'REQ-20260831150439-A861D4'
# MAGIC ORDER BY created_at;
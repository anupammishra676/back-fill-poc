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

# COMMAND ----------

# DBTITLE 1,Cell 2
from datetime import date
import importlib
import src.backfill.backfill_service_module

importlib.reload(src.backfill.backfill_service_module)

from src.backfill.backfill_service_module import BackfillService

# COMMAND ----------

backfill_service = BackfillService(spark)

# COMMAND ----------

# DBTITLE 1,Cell 4
request = backfill_service.submit_request(
    requested_by="demo_user",
    request_text=(
        "Source data for Finance was corrected between "
        "August 20 and August 22. Please reprocess the affected data."
    ),
    business_unit="Finance",
    start_date=date(2026, 8, 20),
    end_date=date(2026, 8, 22),
    risk_level="MEDIUM",
    impact_summary=(
        "Historical Finance data may have incorrect values "
        "due to a source-system correction."
    ),
    proposed_strategy=(
        "Reprocess Bronze, Silver and Gold for the requested "
        "date range."
    ),
    approval_required=True
)

print(request)

# COMMAND ----------

request = backfill_service.submit_request(
    requested_by="demo_user",
    request_text=(
        "The Finance source data for 2026-08-20 through 2026-08-22 was corrected. Please reprocess it."
    ),
    business_unit="Finance",
    start_date=date(2026, 8, 20),
    end_date=date(2026, 8, 22),
    risk_level="MEDIUM",
    impact_summary=(
        "Historical Finance data may have incorrect values "
        "due to a source-system correction."
    ),
    proposed_strategy=(
        "Reprocess Bronze, Silver and Gold for the requested "
        "date range."
    ),
    approval_required=True
)

print(request)
# Databricks notebook source
import sys

PROJECT_ROOT = "/Workspace/Users/anupammishrajsp@gmail.com/DatabricksRepo/Backfill_POC"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

print("Project path configured.")

# COMMAND ----------

from src.ai.backfill_agent_module import BackfillAgent

# COMMAND ----------

api_key = dbutils.secrets.get(
    scope="openai",
    key="api_key"
)

# COMMAND ----------

agent = BackfillAgent(
    api_key=api_key
)

# COMMAND ----------

request_text = """
The Finance source data for 2026-08-20 through 2026-08-22
was corrected. Please reprocess the affected historical data.
The correction impacts the Finance reporting data.
"""

analysis = agent.analyze_request(
    request_text
)

print(analysis)

# COMMAND ----------

request_text = """
The Finance source data for August 20 through
August 22 was corrected. Please reprocess the affected data.
"""

analysis = agent.analyze_request(
    request_text
)

print(analysis)

# COMMAND ----------

request_text = """
Please backfill Finance from 2026-08-25 to 2026-08-20
because the source was corrected.
"""

analysis = agent.analyze_request(
    request_text
)

print(analysis)

# COMMAND ----------

request_text = """
The Finance source data was corrected and needs to be reprocessed.
"""

analysis = agent.analyze_request(request_text)

print(analysis)

# COMMAND ----------

request_text = """
The HR data for 2026-08-10 through 2026-08-12
was corrected. Please reprocess the affected records.
"""

analysis = agent.analyze_request(request_text)

print(analysis)
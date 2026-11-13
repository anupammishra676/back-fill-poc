# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_count
# MAGIC FROM data_engineering.backfill_poc.silver_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_count
# MAGIC FROM data_engineering.backfill_poc.gold_daily_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_count
# MAGIC FROM data_engineering.backfill_poc.gold_product_sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     order_id,
# MAGIC     quantity,
# MAGIC     unit_price,
# MAGIC     discount_pct,
# MAGIC     gross_amount,
# MAGIC     discount_amount,
# MAGIC     net_amount
# MAGIC FROM data_engineering.backfill_poc.silver_orders
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from data_engineering.backfill_poc.pipeline_config

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from data_engineering.backfill_poc.pipeline_run_log

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     COUNT(*) AS invalid_records
# MAGIC FROM data_engineering.backfill_poc.silver_orders
# MAGIC WHERE
# MAGIC     gross_amount < 0
# MAGIC     OR discount_amount < 0
# MAGIC     OR net_amount < 0
# MAGIC     OR discount_amount > gross_amount;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     layer,
# MAGIC     operation,
# MAGIC     status,
# MAGIC     records_read,
# MAGIC     records_written,
# MAGIC     start_time,
# MAGIC     end_time,
# MAGIC     error_message
# MAGIC FROM data_engineering.backfill_poc.pipeline_run_log
# MAGIC WHERE layer = 'SILVER'
# MAGIC ORDER BY created_at DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering.backfill_poc.gold_daily_revenue
# MAGIC ORDER BY business_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering.backfill_poc.gold_product_sales
# MAGIC ORDER BY business_date, product_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(gross_amount) AS silver_gross_revenue,
# MAGIC     SUM(discount_amount) AS silver_discount,
# MAGIC     SUM(net_amount) AS silver_net_revenue
# MAGIC FROM data_engineering.backfill_poc.silver_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     SUM(gross_revenue) AS gold_gross_revenue,
# MAGIC     SUM(discount_amount) AS gold_discount,
# MAGIC     SUM(net_revenue) AS gold_net_revenue
# MAGIC FROM data_engineering.backfill_poc.gold_daily_revenue;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_date,
# MAGIC     product_category,
# MAGIC     total_orders,
# MAGIC     total_units,
# MAGIC     net_revenue
# MAGIC FROM data_engineering.backfill_poc.gold_product_sales
# MAGIC ORDER BY business_date, product_category;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_date,
# MAGIC     SUM(net_revenue) AS product_level_revenue
# MAGIC FROM data_engineering.backfill_poc.gold_product_sales
# MAGIC GROUP BY business_date
# MAGIC ORDER BY business_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     business_date,
# MAGIC     net_revenue
# MAGIC FROM data_engineering.backfill_poc.gold_daily_revenue
# MAGIC ORDER BY business_date;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from data_engineering.backfill_poc.pipeline_run_log 

# COMMAND ----------

import sys

PROJECT_ROOT = "/Workspace/Users/anupammishrajsp@gmail.com/DatabricksRepo/Backfill_POC"

if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

# COMMAND ----------

from src.audit.audit_logger_module import AuditLogger
from src.utils.spark_utils_module import SparkUtils
from datetime import datetime


audit_logger = AuditLogger(spark)

test_run_id = SparkUtils.generate_run_id("TEST")

print(test_run_id)

# COMMAND ----------

audit_logger.log_run(
    run_id=test_run_id,
    pipeline_name="audit_test",
    layer="TEST",
    operation="INITIAL",
    status="STARTED",
    start_time=datetime.now()
)

# COMMAND ----------

audit_logger.log_run(
    run_id=test_run_id,
    pipeline_name="audit_test",
    layer="TEST",
    operation="INITIAL",
    status="SUCCESS",
    start_time=datetime.now(),
    end_time=datetime.now(),
    records_read=100,
    records_written=100
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering.backfill_poc.pipeline_run_log
# MAGIC WHERE run_id = 'TEST-20260829091851-d630b698';

# COMMAND ----------

failed_run_id = SparkUtils.generate_run_id("TEST")

# COMMAND ----------

audit_logger.log_run(
    run_id=failed_run_id,
    pipeline_name="audit_test",
    layer="TEST",
    operation="BACKFILL",
    status="STARTED",
    start_time=datetime.now()
)

# COMMAND ----------

audit_logger.log_run(
    run_id=failed_run_id,
    pipeline_name="audit_test",
    layer="TEST",
    operation="BACKFILL",
    status="FAILED",
    start_time=datetime.now(),
    end_time=datetime.now(),
    records_read=500,
    records_written=0,
    error_message="Simulated validation failure"
)

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM data_engineering.backfill_poc.pipeline_run_log
# MAGIC WHERE pipeline_name IN ('ecommerce_pipeline', 'audit_test');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM data_engineering.backfill_poc.pipeline_run_log;

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE data_engineering.backfill_poc.pipeline_run_log;
# MAGIC
# MAGIC TRUNCATE TABLE data_engineering.backfill_poc.bronze_orders;
# MAGIC
# MAGIC TRUNCATE TABLE data_engineering.backfill_poc.silver_orders;
# MAGIC
# MAGIC TRUNCATE TABLE data_engineering.backfill_poc.gold_daily_revenue;
# MAGIC
# MAGIC TRUNCATE TABLE data_engineering.backfill_poc.gold_product_sales;
# MAGIC
# MAGIC TRUNCATE TABLE data_engineering.backfill_poc.backfill_request;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_name,
# MAGIC     layer,
# MAGIC     source_table,
# MAGIC     target_table,
# MAGIC     partition_column,
# MAGIC     incremental_strategy,
# MAGIC     watermark_column,
# MAGIC     write_strategy,
# MAGIC     active_flag
# MAGIC FROM data_engineering.backfill_poc.pipeline_config
# MAGIC WHERE pipeline_name = 'ecommerce_pipeline'
# MAGIC ORDER BY layer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(order_date) AS min_date,
# MAGIC     MAX(order_date) AS max_date,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM data_engineering.backfill_poc.source_ecommerce_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(order_date) AS min_date,
# MAGIC     MAX(order_date) AS max_date,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM data_engineering.backfill_poc.bronze_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     MIN(order_date) AS min_date,
# MAGIC     MAX(order_date) AS max_date,
# MAGIC     COUNT(*) AS record_count
# MAGIC FROM data_engineering.backfill_poc.bronze_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     pipeline_name,
# MAGIC     layer,
# MAGIC     source_table,
# MAGIC     source_partition_column,
# MAGIC     target_table,
# MAGIC     target_partition_column
# MAGIC FROM data_engineering.backfill_poc.pipeline_config
# MAGIC ORDER BY layer;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     layer,
# MAGIC     source_table,
# MAGIC     source_partition_column,
# MAGIC     target_table,
# MAGIC     target_partition_column
# MAGIC FROM data_engineering.backfill_poc.pipeline_config
# MAGIC WHERE pipeline_name = 'ecommerce_pipeline'
# MAGIC ORDER BY layer;

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from data_engineering.backfill_poc.backfill_request

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from data_engineering.backfill_poc.pipeline_run_log

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history data_engineering.backfill_poc.bronze_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history data_engineering.backfill_poc.silver_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history data_engineering.backfill_poc.gold_daily_revenue 

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history data_engineering.backfill_poc.gold_product_sales  
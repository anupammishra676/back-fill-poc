# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_engineering.backfill_poc.bronze_orders
# MAGIC (
# MAGIC     order_id STRING,
# MAGIC     order_date DATE,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     product_category STRING,
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(12,2),
# MAGIC     discount_pct DECIMAL(5,2),
# MAGIC     payment_status STRING,
# MAGIC     order_status STRING,
# MAGIC     source_updated_at TIMESTAMP,
# MAGIC     ingestion_timestamp TIMESTAMP,
# MAGIC     run_id STRING,
# MAGIC     processing_mode STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_engineering.backfill_poc.silver_orders
# MAGIC (
# MAGIC     order_id STRING,
# MAGIC     order_date DATE,
# MAGIC     customer_id STRING,
# MAGIC     product_id STRING,
# MAGIC     product_category STRING,
# MAGIC     quantity INT,
# MAGIC     unit_price DECIMAL(12,2),
# MAGIC     discount_pct DECIMAL(5,2),
# MAGIC     gross_amount DECIMAL(14,2),
# MAGIC     discount_amount DECIMAL(14,2),
# MAGIC     net_amount DECIMAL(14,2),
# MAGIC     payment_status STRING,
# MAGIC     order_status STRING,
# MAGIC     source_updated_at TIMESTAMP,
# MAGIC     processing_timestamp TIMESTAMP,
# MAGIC     run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_engineering.backfill_poc.gold_daily_revenue
# MAGIC (
# MAGIC     business_date DATE,
# MAGIC     total_orders BIGINT,
# MAGIC     total_units BIGINT,
# MAGIC     gross_revenue DECIMAL(18,2),
# MAGIC     discount_amount DECIMAL(18,2),
# MAGIC     net_revenue DECIMAL(18,2),
# MAGIC     processing_timestamp TIMESTAMP,
# MAGIC     run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_engineering.backfill_poc.gold_product_sales
# MAGIC (
# MAGIC     business_date DATE,
# MAGIC     product_category STRING,
# MAGIC     total_orders BIGINT,
# MAGIC     total_units BIGINT,
# MAGIC     gross_revenue DECIMAL(18,2),
# MAGIC     discount_amount DECIMAL(18,2),
# MAGIC     net_revenue DECIMAL(18,2),
# MAGIC     processing_timestamp TIMESTAMP,
# MAGIC     run_id STRING
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE or REPLACE TABLE data_engineering.backfill_poc.pipeline_config
# MAGIC (
# MAGIC     pipeline_name STRING,
# MAGIC     layer STRING,
# MAGIC     source_table STRING,
# MAGIC     target_table STRING,
# MAGIC     partition_column STRING,
# MAGIC     incremental_strategy STRING,
# MAGIC     watermark_column STRING,
# MAGIC     write_strategy STRING,
# MAGIC     active_flag BOOLEAN,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO data_engineering.backfill_poc.pipeline_config
# MAGIC (
# MAGIC     pipeline_name,
# MAGIC     layer,
# MAGIC     source_table,
# MAGIC     target_table,
# MAGIC     partition_column,
# MAGIC     incremental_strategy,
# MAGIC     watermark_column,
# MAGIC     write_strategy,
# MAGIC     active_flag,
# MAGIC     created_at,
# MAGIC     updated_at
# MAGIC )
# MAGIC VALUES
# MAGIC
# MAGIC (
# MAGIC     'ecommerce_pipeline',
# MAGIC     'BRONZE',
# MAGIC     'data_engineering.backfill_poc.source_ecommerce_orders',
# MAGIC     'data_engineering.backfill_poc.bronze_orders',
# MAGIC     'order_date',
# MAGIC     'PARTITION',
# MAGIC     NULL,
# MAGIC     'APPEND',
# MAGIC     TRUE,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'ecommerce_pipeline',
# MAGIC     'SILVER',
# MAGIC     'data_engineering.backfill_poc.bronze_orders',
# MAGIC     'data_engineering.backfill_poc.silver_orders',
# MAGIC     'order_date',
# MAGIC     'PARTITION',
# MAGIC     NULL,
# MAGIC     'MERGE',
# MAGIC     TRUE,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'ecommerce_pipeline',
# MAGIC     'GOLD_DAILY_REVENUE',
# MAGIC     'data_engineering.backfill_poc.silver_orders',
# MAGIC     'data_engineering.backfill_poc.gold_daily_revenue',
# MAGIC     'business_date',
# MAGIC     'PARTITION',
# MAGIC     NULL,
# MAGIC     'MERGE',
# MAGIC     TRUE,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()
# MAGIC ),
# MAGIC
# MAGIC (
# MAGIC     'ecommerce_pipeline',
# MAGIC     'GOLD_PRODUCT_SALES',
# MAGIC     'data_engineering.backfill_poc.silver_orders',
# MAGIC     'data_engineering.backfill_poc.gold_product_sales',
# MAGIC     'business_date',
# MAGIC     'PARTITION',
# MAGIC     NULL,
# MAGIC     'MERGE',
# MAGIC     TRUE,
# MAGIC     current_timestamp(),
# MAGIC     current_timestamp()
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE data_engineering.backfill_poc.pipeline_execution_state
# MAGIC (
# MAGIC     pipeline_name STRING,
# MAGIC     layer STRING,
# MAGIC     last_processed_value STRING,
# MAGIC     last_run_id STRING,
# MAGIC     last_run_status STRING,
# MAGIC     last_execution_time TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_engineering.backfill_poc.pipeline_run_log
# MAGIC (
# MAGIC     run_id STRING,
# MAGIC     pipeline_name STRING,
# MAGIC     layer STRING,
# MAGIC     operation STRING,
# MAGIC     status STRING,
# MAGIC     start_time TIMESTAMP,
# MAGIC     end_time TIMESTAMP,
# MAGIC     records_read BIGINT,
# MAGIC     records_written BIGINT,
# MAGIC     processing_start_date DATE,
# MAGIC     processing_end_date DATE,
# MAGIC     parameters STRING,
# MAGIC     error_message STRING,
# MAGIC     created_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_engineering.backfill_poc.backfill_request
# MAGIC (
# MAGIC     request_id STRING,
# MAGIC     requested_by STRING,
# MAGIC     request_text STRING,
# MAGIC     business_unit STRING,
# MAGIC     start_date DATE,
# MAGIC     end_date DATE,
# MAGIC     status STRING,
# MAGIC     risk_level STRING,
# MAGIC     impact_summary STRING,
# MAGIC     proposed_strategy STRING,
# MAGIC     approval_required BOOLEAN,
# MAGIC     approved_by STRING,
# MAGIC     approved_at TIMESTAMP,
# MAGIC     created_at TIMESTAMP,
# MAGIC     updated_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in data_engineering.backfill_poc

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM data_engineering.backfill_poc.source_ecommerce_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*)
# MAGIC FROM data_engineering.backfill_poc.bronze_orders;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE data_engineering.backfill_poc.pipeline_run_log
# MAGIC ADD COLUMNS (
# MAGIC     request_id STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE data_engineering.backfill_poc.backfill_request
# MAGIC ADD COLUMNS (
# MAGIC     rejected_by STRING,
# MAGIC     rejected_at TIMESTAMP,
# MAGIC     rejection_reason STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE data_engineering.backfill_poc.backfill_request
# MAGIC ADD COLUMNS (
# MAGIC     execution_id STRING,
# MAGIC     execution_started_at TIMESTAMP,
# MAGIC     execution_completed_at TIMESTAMP,
# MAGIC     execution_error STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE data_engineering.backfill_poc.pipeline_config
# MAGIC SET write_strategy = 'REPLACE_WINDOW_APPEND',
# MAGIC     updated_at = current_timestamp()
# MAGIC WHERE layer IN (
# MAGIC     'SILVER',
# MAGIC     'GOLD_DAILY_REVENUE',
# MAGIC     'GOLD_PRODUCT_SALES'
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE data_engineering.backfill_poc.pipeline_config
# MAGIC ADD COLUMNS (
# MAGIC     source_partition_column STRING,
# MAGIC     target_partition_column STRING
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE data_engineering.backfill_poc.pipeline_config
# MAGIC SET
# MAGIC     source_partition_column = partition_column,
# MAGIC     target_partition_column = partition_column;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE data_engineering.backfill_poc.pipeline_config
# MAGIC SET
# MAGIC     source_partition_column = 'order_date',
# MAGIC     target_partition_column = 'business_date',
# MAGIC     updated_at = current_timestamp()
# MAGIC WHERE pipeline_name = 'ecommerce_pipeline'
# MAGIC   AND layer IN (
# MAGIC       'GOLD_DAILY_REVENUE',
# MAGIC       'GOLD_PRODUCT_SALES'
# MAGIC   );

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE data_engineering.backfill_poc.backfill_request
# MAGIC ADD COLUMNS (
# MAGIC     risk_reason STRING
# MAGIC );
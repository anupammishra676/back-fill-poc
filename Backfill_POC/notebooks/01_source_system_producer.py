# Databricks notebook source
spark.sql("""
CREATE SCHEMA IF NOT EXISTS data_engineering.backfill_poc
""")

print("Schema ready: backfill_poc")

# COMMAND ----------

# ============================================================
# NOTEBOOK PARAMETERS
# ============================================================

dbutils.widgets.dropdown(
    "operation",
    "GENERATE",
    ["GENERATE", "CORRECT", "LATE_ARRIVAL"]
)

dbutils.widgets.text(
    "start_date",
    "2026-08-01"
)

dbutils.widgets.text(
    "end_date",
    "2026-08-10"
)

dbutils.widgets.text(
    "num_records",
    "10000"
)

dbutils.widgets.text(
    "correction_percentage",
    "5"
)

dbutils.widgets.text(
    "late_records",
    "500"
)


# Read parameters
operation = dbutils.widgets.get("operation").upper()
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")

num_records = int(dbutils.widgets.get("num_records"))
correction_percentage = int(
    dbutils.widgets.get("correction_percentage")
)
late_records = int(
    dbutils.widgets.get("late_records")
)

print(f"Operation             : {operation}")
print(f"Start Date            : {start_date}")
print(f"End Date              : {end_date}")
print(f"Records               : {num_records}")
print(f"Correction %          : {correction_percentage}")
print(f"Late Records          : {late_records}")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import types as T

SOURCE_TABLE = "data_engineering.backfill_poc.source_ecommerce_orders"

print(f"Source table: {SOURCE_TABLE}")

# COMMAND ----------

source_schema = T.StructType([
    T.StructField("order_id", T.StringType(), False),
    T.StructField("order_date", T.DateType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("product_category", T.StringType(), False),
    T.StructField("quantity", T.IntegerType(), False),
    T.StructField("unit_price", T.DecimalType(12, 2), False),
    T.StructField("discount_pct", T.DecimalType(5, 2), False),
    T.StructField("payment_status", T.StringType(), False),
    T.StructField("order_status", T.StringType(), False),
    T.StructField("source_updated_at", T.TimestampType(), False)
])

# COMMAND ----------

# ============================================================
# GENERATE INITIAL SOURCE DATA
# ============================================================

if operation == "GENERATE":

    print("Generating initial source data...")

    # Create deterministic record IDs
    df = (
        spark.range(num_records)
        .withColumnRenamed("id", "record_id")
    )

    # --------------------------------------------------------
    # Order ID
    # --------------------------------------------------------

    df = df.withColumn(
        "order_id",
        F.concat(
            F.lit("ORD-"),
            F.lpad(
                F.col("record_id").cast("string"),
                8,
                "0"
            )
        )
    )

    # --------------------------------------------------------
    # Order date
    # --------------------------------------------------------

    days_in_range = (
        F.datediff(
            F.to_date(F.lit(end_date)),
            F.to_date(F.lit(start_date))
        ) + 1
    )

    df = df.withColumn(
        "order_date",
        F.date_add(
            F.to_date(F.lit(start_date)),
            F.pmod(
                F.col("record_id"),
                days_in_range
            ).cast("int")
        )
    )

    # --------------------------------------------------------
    # Customer
    # --------------------------------------------------------

    df = df.withColumn(
        "customer_id",
        F.concat(
            F.lit("CUST-"),
            F.lpad(
                ((F.col("record_id") % 5000) + 1)
                .cast("string"),
                5,
                "0"
            )
        )
    )

    # --------------------------------------------------------
    # Product
    # --------------------------------------------------------

    df = df.withColumn(
        "product_id",
        F.concat(
            F.lit("PROD-"),
            F.lpad(
                ((F.col("record_id") % 1000) + 1)
                .cast("string"),
                4,
                "0"
            )
        )
    )

    # --------------------------------------------------------
    # Product category
    # --------------------------------------------------------

    categories = [
        "Electronics",
        "Clothing",
        "Home",
        "Beauty",
        "Sports",
        "Books"
    ]

    category_expr = F.array(
        *[F.lit(x) for x in categories]
    )

    df = df.withColumn(
        "product_category",
        F.element_at(
            category_expr,
            (F.pmod(F.col("record_id"), 6) + 1).cast("int")
        )
    )

    # --------------------------------------------------------
    # Quantity
    # --------------------------------------------------------

    df = df.withColumn(
        "quantity",
        (
            F.pmod(F.col("record_id"), 5) + 1
        ).cast("int")
    )

    # --------------------------------------------------------
    # Unit price
    # --------------------------------------------------------

    df = df.withColumn(
        "unit_price",
        F.round(
            F.lit(20) +
            (F.pmod(F.col("record_id") * 37, 980)).cast("double"),
            2
        ).cast("decimal(12,2)")
    )

    # --------------------------------------------------------
    # Discount
    # --------------------------------------------------------

    df = df.withColumn(
        "discount_pct",
        F.when(
            F.pmod(F.col("record_id"), 5) == 0,
            F.lit(10.00)
        )
        .when(
            F.pmod(F.col("record_id"), 5) == 1,
            F.lit(5.00)
        )
        .otherwise(F.lit(0.00))
        .cast("decimal(5,2)")
    )

    # --------------------------------------------------------
    # Payment status
    # --------------------------------------------------------

    df = df.withColumn(
        "payment_status",
        F.when(
            F.pmod(F.col("record_id"), 20) == 0,
            "PENDING"
        ).otherwise("PAID")
    )

    # --------------------------------------------------------
    # Order status
    # --------------------------------------------------------

    df = df.withColumn(
        "order_status",
        F.when(
            F.pmod(F.col("record_id"), 10) == 0,
            "CANCELLED"
        )
        .when(
            F.pmod(F.col("record_id"), 3) == 0,
            "SHIPPED"
        )
        .otherwise("COMPLETED")
    )

    # --------------------------------------------------------
    # Source timestamp
    # --------------------------------------------------------

    df = df.withColumn(
        "source_updated_at",
        F.current_timestamp()
    )

    # --------------------------------------------------------
    # Select final columns
    # --------------------------------------------------------

    df = df.select(
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "product_category",
        "quantity",
        "unit_price",
        "discount_pct",
        "payment_status",
        "order_status",
        "source_updated_at"
    )

    # --------------------------------------------------------
    # Write source table
    # --------------------------------------------------------

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(SOURCE_TABLE)
    )

    print("Initial source data generated successfully.")
    print(f"Records generated: {df.count()}")

# COMMAND ----------

display(
    spark.table(SOURCE_TABLE)
    .orderBy("order_date", "order_id")
    .limit(50)
)

# COMMAND ----------

# ============================================================
# CORRECT EXISTING SOURCE DATA
# ============================================================

if operation == "CORRECT":

    print("Simulating source corrections...")

    source_df = spark.table(SOURCE_TABLE)

    # Find records in requested date range
    affected_df = (
        source_df
        .filter(
            (F.col("order_date") >= F.to_date(F.lit(start_date))) &
            (F.col("order_date") <= F.to_date(F.lit(end_date)))
        )
        .withColumn(
            "correction_bucket",
            F.pmod(
                F.xxhash64("order_id"),
                F.lit(100)
            )
        )
    )

    # Select approximately requested percentage
    corrected_df = (
        affected_df
        .filter(
            F.col("correction_bucket") < correction_percentage
        )
        .drop("correction_bucket")
        .withColumn(
            "quantity",
            F.col("quantity") + 1
        )
        .withColumn(
            "source_updated_at",
            F.current_timestamp()
        )
    )

    corrected_count = corrected_df.count()

    print(f"Records selected for correction: {corrected_count}")

    # Create temporary view
    corrected_df.createOrReplaceTempView(
        "corrected_orders"
    )

    # Update source table
    spark.sql(f"""
        MERGE INTO {SOURCE_TABLE} AS target

        USING corrected_orders AS source

        ON target.order_id = source.order_id

        WHEN MATCHED THEN UPDATE SET
            target.order_date = source.order_date,
            target.customer_id = source.customer_id,
            target.product_id = source.product_id,
            target.product_category = source.product_category,
            target.quantity = source.quantity,
            target.unit_price = source.unit_price,
            target.discount_pct = source.discount_pct,
            target.payment_status = source.payment_status,
            target.order_status = source.order_status,
            target.source_updated_at = source.source_updated_at
    """)

    print("Source correction completed.")

# COMMAND ----------

# ============================================================
# GENERATE LATE-ARRIVING RECORDS
# ============================================================

if operation == "LATE_ARRIVAL":

    print("Generating late-arriving records...")

    existing_df = spark.table(SOURCE_TABLE)

    # Get next numeric order ID
    max_order_number = (
        existing_df
        .select(
            F.max(
                F.regexp_extract(
                    "order_id",
                    r"ORD-(\d+)",
                    1
                ).cast("long")
            ).alias("max_id")
        )
        .collect()[0]["max_id"]
    )

    if max_order_number is None:
        max_order_number = 0

    print(f"Current maximum order ID: {max_order_number}")

    late_df = (
        spark.range(late_records)
        .withColumnRenamed("id", "record_id")
    )

    # --------------------------------------------------------
    # Generate unique order ID
    # --------------------------------------------------------

    late_df = late_df.withColumn(
        "order_id",
        F.concat(
            F.lit("ORD-"),
            F.lpad(
                (
                    F.col("record_id") +
                    max_order_number +
                    1
                ).cast("string"),
                8,
                "0"
            )
        )
    )

    # --------------------------------------------------------
    # Late arrival date
    # --------------------------------------------------------

    late_df = late_df.withColumn(
        "order_date",
        F.date_sub(
            F.to_date(F.lit(start_date)),
            (F.pmod(F.col("record_id"), 3) + 1).cast("int")
        )
    )

    late_df = (
        late_df
        .withColumn(
            "customer_id",
            F.concat(
                F.lit("CUST-LATE-"),
                F.lpad(
                    F.col("record_id").cast("string"),
                    5,
                    "0"
                )
            )
        )
        .withColumn(
            "product_id",
            F.concat(
                F.lit("PROD-"),
                F.lpad(
                    ((F.col("record_id") % 1000) + 1)
                    .cast("string"),
                    4,
                    "0"
                )
            )
        )
        .withColumn(
            "product_category",
            F.element_at(
                F.array(
                    F.lit("Electronics"),
                    F.lit("Clothing"),
                    F.lit("Home"),
                    F.lit("Beauty"),
                    F.lit("Sports"),
                    F.lit("Books")
                ),
                (F.pmod(F.col("record_id"), 6) + 1)
                .cast("int")
            )
        )
        .withColumn(
            "quantity",
            (F.pmod(F.col("record_id"), 5) + 1)
            .cast("int")
        )
        .withColumn(
            "unit_price",
            F.round(
                F.lit(20) +
                F.pmod(
                    F.col("record_id") * 37,
                    980
                ),
                2
            ).cast("decimal(12,2)")
        )
        .withColumn(
            "discount_pct",
            F.lit(5.00).cast("decimal(5,2)")
        )
        .withColumn(
            "payment_status",
            F.lit("PAID")
        )
        .withColumn(
            "order_status",
            F.lit("COMPLETED")
        )
        .withColumn(
            "source_updated_at",
            F.current_timestamp()
        )
    )

    late_df = late_df.select(
        "order_id",
        "order_date",
        "customer_id",
        "product_id",
        "product_category",
        "quantity",
        "unit_price",
        "discount_pct",
        "payment_status",
        "order_status",
        "source_updated_at"
    )

    # Append late records
    (
        late_df.write
        .format("delta")
        .mode("append")
        .saveAsTable(SOURCE_TABLE)
    )

    print(f"Late-arriving records added: {late_records}")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from data_engineering.backfill_poc.source_ecommerce_orders limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table data_engineering.backfill_poc.source_ecommerce_orders

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO data_engineering.backfill_poc.source_ecommerce_orders
# MAGIC (order_id, order_date, customer_id, product_id, product_category,
# MAGIC  quantity, unit_price, discount_pct, payment_status, order_status, source_updated_at)
# MAGIC VALUES
# MAGIC ('ORD-10001', '2026-08-20', 'CUST-001', 'PROD-101', 'Electronics',
# MAGIC  2, 599.99, 10.00, 'PAID', 'COMPLETED', '2026-08-28 10:15:00'),
# MAGIC
# MAGIC ('ORD-10002', '2026-08-20', 'CUST-002', 'PROD-205', 'Furniture',
# MAGIC  1, 1299.50, 5.00, 'PAID', 'SHIPPED', '2026-08-28 11:30:00'),
# MAGIC
# MAGIC ('ORD-10003', '2026-08-21', 'CUST-003', 'PROD-310', 'Clothing',
# MAGIC  3, 49.99, 15.00, 'PAID', 'COMPLETED', '2026-08-21 09:45:00'),
# MAGIC
# MAGIC ('ORD-10004', '2026-08-21', 'CUST-001', 'PROD-415', 'Home Appliances',
# MAGIC  1, 799.00, 0.00, 'PENDING', 'PROCESSING', '2026-08-21 14:20:00'),
# MAGIC
# MAGIC ('ORD-10005', '2026-08-22', 'CUST-004', 'PROD-520', 'Sports',
# MAGIC  2, 89.95, 20.00, 'PAID', 'SHIPPED', '2026-08-22 08:10:00'),
# MAGIC
# MAGIC ('ORD-10006', '2026-08-22', 'CUST-005', 'PROD-125', 'Electronics',
# MAGIC  1, 1499.99, 10.00, 'FAILED', 'CANCELLED', '2026-08-22 16:35:00'),
# MAGIC
# MAGIC ('ORD-10007', '2026-08-23', 'CUST-006', 'PROD-230', 'Clothing',
# MAGIC  4, 29.99, 5.00, 'PAID', 'COMPLETED', '2026-08-23 12:05:00'),
# MAGIC
# MAGIC ('ORD-10008', '2026-08-24', 'CUST-002', 'PROD-601', 'Furniture',
# MAGIC  1, 2499.00, 12.50, 'PAID', 'PROCESSING', '2026-08-24 13:40:00');
from dataclasses import dataclass


@dataclass(frozen=True)
class TableConfig:
    source_orders: str = (
        "data_engineering.backfill_poc.source_ecommerce_orders"
    )

    bronze_orders: str = (
        "data_engineering.backfill_poc.bronze_orders"
    )

    silver_orders: str = (
        "data_engineering.backfill_poc.silver_orders"
    )

    gold_daily_revenue: str = (
        "data_engineering.backfill_poc.gold_daily_revenue"
    )

    gold_product_sales: str = (
        "data_engineering.backfill_poc.gold_product_sales"
    )

    pipeline_log: str = (
        "data_engineering.backfill_poc.pipeline_run_log"
    )

    pipeline_config: str = (
        "data_engineering.backfill_poc.pipeline_config"
    )

    backfill_request: str = (
        "data_engineering.backfill_poc.backfill_request"
    )


@dataclass(frozen=True)
class ApplicationConfig:
    application_name: str = "ecommerce_backfill_platform"
    pipeline_name: str = "ecommerce_pipeline"
    environment: str = "dev"

    catalog: str = "data_engineering"
    schema: str = "backfill_poc"

    tables: TableConfig = TableConfig()


CONFIG = ApplicationConfig()

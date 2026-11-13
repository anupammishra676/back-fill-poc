from dataclasses import dataclass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col


@dataclass
class PipelineConfigRecord:
    pipeline_name: str
    layer: str
    source_table: str
    target_table: str
    partition_column: str
    source_partition_column: str
    target_partition_column: str
    incremental_strategy: str
    watermark_column: str
    write_strategy: str
    active_flag: bool


class PipelineConfig:

    CONFIG_TABLE = (
        "data_engineering.backfill_poc.pipeline_config"
    )

    def __init__(self, spark: SparkSession):
        self.spark = spark

    def get_config(
        self,
        pipeline_name: str,
        layer: str
    ) -> PipelineConfigRecord:

        config_df = (
            self.spark.table(self.CONFIG_TABLE)
            .filter(
                (col("pipeline_name") == pipeline_name)
                & (col("layer") == layer)
                & (col("active_flag") == True)
            )
        )

        config = config_df.first()

        if config is None:
            raise ValueError(
                "No active pipeline configuration found | "
                f"pipeline_name={pipeline_name} | "
                f"layer={layer}"
            )

        return PipelineConfigRecord(
            pipeline_name=config.pipeline_name,
            layer=config.layer,
            source_table=config.source_table,
            target_table=config.target_table,
            partition_column=config.partition_column,
            source_partition_column=config.source_partition_column,
            target_partition_column=config.target_partition_column,
            incremental_strategy=config.incremental_strategy,
            watermark_column=config.watermark_column,
            write_strategy=config.write_strategy,
            active_flag=config.active_flag
        )
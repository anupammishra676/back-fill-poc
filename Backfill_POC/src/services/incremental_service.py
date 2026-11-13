from dataclasses import dataclass
from datetime import timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max

from src.config.pipeline_config import PipelineConfig


@dataclass
class IncrementalWindow:
    start_date: object
    end_date: object

    @property
    def has_data(self) -> bool:
        return (
            self.start_date is not None
            and self.end_date is not None
            and self.start_date <= self.end_date
        )


class IncrementalService:

    def __init__(self, spark: SparkSession):

        self.spark = spark

        self.pipeline_config = PipelineConfig(
            spark
        )

    def get_incremental_window(
        self,
        pipeline_name: str,
        layer: str
    ) -> IncrementalWindow:

        config = self.pipeline_config.get_config(
            pipeline_name=pipeline_name,
            layer=layer
        )

        if config.incremental_strategy != "PARTITION":

            raise ValueError(
                "Unsupported incremental strategy | "
                f"strategy={config.incremental_strategy} | "
                f"pipeline_name={pipeline_name} | "
                f"layer={layer}"
            )

        if not config.source_partition_column:

            raise ValueError(
                "Source partition column is required for "
                "PARTITION incremental strategy | "
                f"pipeline_name={pipeline_name} | "
                f"layer={layer}"
            )

        if not config.target_partition_column:

            raise ValueError(
                "Target partition column is required for "
                "PARTITION incremental strategy | "
                f"pipeline_name={pipeline_name} | "
                f"layer={layer}"
            )

        source_max = self._get_max_value(
            config.source_table,
            config.source_partition_column
        )

        target_max = self._get_max_value(
            config.target_table,
            config.target_partition_column
        )

        if source_max is None:

            return IncrementalWindow(
                None,
                None
            )

        if target_max is None:

            raise ValueError(
                "Target table contains no processed data. "
                "Run INITIAL load before INCREMENTAL processing | "
                f"target_table={config.target_table}"
            )

        start_date = (
            target_max + timedelta(days=1)
        )

        end_date = source_max

        if start_date > end_date:

            return IncrementalWindow(
                None,
                None
            )

        return IncrementalWindow(
            start_date,
            end_date
        )

    def _get_max_value(
        self,
        table_name,
        column_name
    ):

        result = (
            self.spark
            .table(table_name)
            .select(
                spark_max(
                    col(column_name)
                ).alias("max_value")
            )
            .first()
        )

        return result["max_value"]
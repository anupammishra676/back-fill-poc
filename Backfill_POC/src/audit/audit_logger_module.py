from datetime import datetime, date
import json

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    LongType,
    DateType
)

from src.config.config_module import CONFIG
from src.utils.logging_utils_module import ApplicationLogger


class AuditLogger:

    """
    Production-style audit logger.

    Audit lifecycle:

        STARTED
           ↓
        SUCCESS / FAILED

    One workflow execution + one layer + one operation
    produces exactly ONE audit record.

    STARTED:
        Inserts the audit record.

    SUCCESS / FAILED:
        Updates the existing audit record.
    """

    TERMINAL_STATUSES = {
        "SUCCESS",
        "FAILED"
    }

    def __init__(self, spark):

        self.spark = spark

        self.logger = ApplicationLogger.get_logger(
            self.__class__.__name__
        )

        self.audit_table = CONFIG.tables.pipeline_log

    # =========================================================
    # PUBLIC METHOD
    # =========================================================

    def log_run(
        self,
        run_id: str,
        request_id: str,
        pipeline_name: str,
        layer: str,
        operation: str,
        status: str,
        start_time: datetime,
        end_time: datetime = None,
        records_read: int = 0,
        records_written: int = 0,
        processing_start_date=None,
        processing_end_date=None,
        parameters: dict = None,
        error_message: str = None
    ):
        """
        Insert or update one pipeline execution record.

        STARTED:
            Inserts a new audit record.

        SUCCESS / FAILED:
            Updates the existing audit record.

        Match key:
            run_id + pipeline_name + layer + operation
        """

        status = status.upper()

        if not run_id:
            raise ValueError(
                "run_id cannot be null or empty."
            )

        if not pipeline_name:
            raise ValueError(
                "pipeline_name cannot be null or empty."
            )

        if not layer:
            raise ValueError(
                "layer cannot be null or empty."
            )

        if not operation:
            raise ValueError(
                "operation cannot be null or empty."
            )

        if not status:
            raise ValueError(
                "status cannot be null or empty."
            )

        try:

            parameter_string = (
                json.dumps(parameters)
                if parameters
                else None
            )

            records_read = int(records_read or 0)
            records_written = int(records_written or 0)

            # Convert string dates to date objects
            if processing_start_date is not None:
                if isinstance(processing_start_date, str):
                    processing_start_date = datetime.strptime(
                        processing_start_date, "%Y-%m-%d"
                    ).date()
                elif isinstance(processing_start_date, datetime):
                    processing_start_date = processing_start_date.date()

            if processing_end_date is not None:
                if isinstance(processing_end_date, str):
                    processing_end_date = datetime.strptime(
                        processing_end_date, "%Y-%m-%d"
                    ).date()
                elif isinstance(processing_end_date, datetime):
                    processing_end_date = processing_end_date.date()

            data = [(
                run_id,
                request_id,
                pipeline_name,
                layer,
                operation,
                status,
                start_time,
                end_time,
                records_read,
                records_written,
                processing_start_date,
                processing_end_date,
                parameter_string,
                error_message,
                datetime.now()
            )]

            schema = StructType([
                StructField(
                    "run_id",
                    StringType(),
                    True
                ),
                StructField("request_id", StringType(), True),
                StructField(
                    "pipeline_name",
                    StringType(),
                    True
                ),
                StructField(
                    "layer",
                    StringType(),
                    True
                ),
                StructField(
                    "operation",
                    StringType(),
                    True
                ),
                StructField(
                    "status",
                    StringType(),
                    True
                ),
                StructField(
                    "start_time",
                    TimestampType(),
                    True
                ),
                StructField(
                    "end_time",
                    TimestampType(),
                    True
                ),
                StructField(
                    "records_read",
                    LongType(),
                    True
                ),
                StructField(
                    "records_written",
                    LongType(),
                    True
                ),
                StructField(
                    "processing_start_date",
                    DateType(),
                    True
                ),
                StructField(
                    "processing_end_date",
                    DateType(),
                    True
                ),
                StructField(
                    "parameters",
                    StringType(),
                    True
                ),
                StructField(
                    "error_message",
                    StringType(),
                    True
                ),
                StructField(
                    "created_at",
                    TimestampType(),
                    True
                )
            ])

            audit_df = self.spark.createDataFrame(
                data,
                schema
            )

            # -------------------------------------------------
            # Temporary view
            # -------------------------------------------------

            audit_df.createOrReplaceTempView(
                "audit_log_source"
            )

            # -------------------------------------------------
            # MERGE
            # -------------------------------------------------

            merge_sql = f"""
                MERGE INTO {self.audit_table} AS target

                USING audit_log_source AS source

                ON  target.run_id = source.run_id
                AND target.pipeline_name = source.pipeline_name
                AND target.layer = source.layer
                AND target.operation = source.operation

                WHEN MATCHED THEN
                    UPDATE SET

                        target.request_id = 
                            source.request_id,

                        target.status =
                            source.status,

                        target.end_time =
                            source.end_time,

                        target.records_read =
                            source.records_read,

                        target.records_written =
                            source.records_written,

                        target.processing_start_date =
                            source.processing_start_date,

                        target.processing_end_date =
                            source.processing_end_date,

                        target.parameters =
                            source.parameters,

                        target.error_message =
                            source.error_message

                WHEN NOT MATCHED THEN
                    INSERT (
                        run_id,
                        request_id,
                        pipeline_name,
                        layer,
                        operation,
                        status,
                        start_time,
                        end_time,
                        records_read,
                        records_written,
                        processing_start_date,
                        processing_end_date,
                        parameters,
                        error_message,
                        created_at
                    )

                    VALUES (
                        source.run_id,
                        source.request_id,
                        source.pipeline_name,
                        source.layer,
                        source.operation,
                        source.status,
                        source.start_time,
                        source.end_time,
                        source.records_read,
                        source.records_written,
                        source.processing_start_date,
                        source.processing_end_date,
                        source.parameters,
                        source.error_message,
                        source.created_at
                    )
            """

            self.spark.sql(merge_sql)

            self.logger.info(
                f"Audit record upserted | "
                f"run_id={run_id} | "
                f"request_id={request_id} | "
                f"layer={layer} | "
                f"operation={operation} | "
                f"status={status}"
            )

        except Exception:

            self.logger.exception(
                f"Failed to upsert audit record | "
                f"run_id={run_id} | "
                f"request_id={request_id} | "
                f"layer={layer} | "
                f"operation={operation}"
            )

            raise
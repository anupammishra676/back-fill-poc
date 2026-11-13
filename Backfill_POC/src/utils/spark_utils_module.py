import uuid
from datetime import datetime


class SparkUtils:

    @staticmethod
    def generate_run_id(prefix: str = "WF") -> str:
        """
        Generate a unique workflow execution ID.

        This ID should normally be generated once by the
        workflow/orchestrator and passed to every layer.
        """

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]

        return f"{prefix}-{timestamp}-{unique_id}"

    @staticmethod
    def validate_table_exists(spark, table_name: str) -> bool:
        """
        Validate that a Spark/Delta table exists.
        """

        return spark.catalog.tableExists(table_name)

    @staticmethod
    def get_table_count(spark, table_name: str) -> int:
        """
        Return the number of records in a table.
        """

        return spark.table(table_name).count()
class JobParameters:

    @staticmethod
    def get_required(dbutils, parameter_name: str) -> str:
        """
        Retrieve a required Databricks job parameter.
        """

        value = dbutils.widgets.get(parameter_name)

        if value is None or value.strip() == "":
            raise ValueError(
                f"Required job parameter is missing: "
                f"{parameter_name}"
            )

        return value

    @staticmethod
    def get_optional(
        dbutils,
        parameter_name: str,
        default=None
    ):
        """
        Retrieve an optional Databricks job parameter.
        """

        try:

            value = dbutils.widgets.get(parameter_name)

            if value is None or value.strip() == "":
                return default

            return value

        except Exception:

            return default
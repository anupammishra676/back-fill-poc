from datetime import datetime
import requests
import time

from databricks.sdk.runtime import dbutils

from src.config.config_module import CONFIG
from src.utils.logging_utils_module import ApplicationLogger


class BackfillExecutionService:

    JOB_ID = 655023774126314
    JOB_NAME = "backfill_poc_pipeline"

    def __init__(self, spark):

        self.spark = spark

        self.logger = ApplicationLogger.get_logger(
            self.__class__.__name__
        )

        self.backfill_request_table = (
            CONFIG.tables.backfill_request
        )

    # =========================================================
    # Get Databricks workspace URL
    # =========================================================

    def get_workspace_url(self) -> str:

        return (
            dbutils.notebook.entry_point
            .getDbutils()
            .notebook()
            .getContext()
            .apiUrl()
            .get()
        )

    # =========================================================
    # Get temporary notebook context token
    # =========================================================

    def get_api_token(self) -> str:

        return (
            dbutils.notebook.entry_point
            .getDbutils()
            .notebook()
            .getContext()
            .apiToken()
            .get()
        )

    # =========================================================
    # Validate request before execution
    # =========================================================

    def validate_request_for_execution(self,request_id: str):
        """
        Validate whether a backfill request is eligible for execution.

        Validation order:
        1. Request must exist.
        2. Request must not have an execution already associated with it.
        3. Request must be APPROVED.
        """

        request_df = (
            self.spark.table(
            self.backfill_request_table
            )
            .filter(
            f"request_id = '{request_id}'"
            )
        )

        requests = request_df.collect()

        if not requests:

            raise ValueError(
            f"Backfill request not found | "
            f"request_id={request_id}"
            )

        request = requests[0]

    # ---------------------------------------------------------
    # Idempotency protection
    # ---------------------------------------------------------
    # Check this BEFORE approval status.
    #
    # If execution_id already exists, this request has already
    # been submitted to Databricks and must never be submitted
    # again.
    # ---------------------------------------------------------

        if (
            request.execution_id is not None
            and request.status in {
                                "RUNNING",
                                "EXECUTION_REQUESTED",
                                "SUCCESS"
                            }
            ):

            raise ValueError(
            f"Backfill request has already been "
            f"executed or execution was initiated | "
            f"request_id={request_id} | "
            f"execution_id={request.execution_id} | "
            f"status={request.status}"
        )

    # ---------------------------------------------------------
    # Approval validation
    # ---------------------------------------------------------

        if request.status not in {"APPROVED", "FAILED"}:

            raise ValueError(
            f"Backfill request is not eligible for execution | "
            f"request_id={request_id} | "
            f"status={request.status}"
        )

        return request

    # =========================================================
    # Build Databricks job parameters
    # =========================================================

    def build_job_parameters(
        self,
        request
    ) -> dict:

        parameters = {
            "mode": "BACKFILL",
            "start_date": str(request.start_date),
            "end_date": str(request.end_date),
            "request_id": str(request.request_id)
        }

        self.logger.info(
            "Backfill execution parameters created | "
            f"request_id={request.request_id} | "
            f"parameters={parameters}"
        )

        return parameters

    # =========================================================
    # Update backfill request status
    # =========================================================

    def update_request_status(
        self,
        request_id: str,
        status: str,
        execution_id=None,
        execution_started_at=None,
        execution_completed_at=None,
        execution_error=None
    ):
        """
        Update the execution status of a backfill request.

        Lifecycle:

            APPROVED
                ↓
            EXECUTION_REQUESTED
                ↓
            RUNNING
                ↓
            SUCCESS / FAILED / TIMEOUT

        Only values explicitly supplied to this method
        are updated.
        """

        try:

            updated_at = datetime.now()

            set_clauses = [
                f"status = '{status}'",
                f"updated_at = TIMESTAMP('{updated_at}')"
            ]

            # -------------------------------------------------
            # Execution ID
            # -------------------------------------------------

            if execution_id is not None:

                set_clauses.append(
                    f"execution_id = '{str(execution_id)}'"
                )

            # -------------------------------------------------
            # Execution started timestamp
            # -------------------------------------------------

            if execution_started_at is not None:

                set_clauses.append(
                    f"execution_started_at = "
                    f"TIMESTAMP('{execution_started_at}')"
                )

            # -------------------------------------------------
            # Execution completed timestamp
            # -------------------------------------------------

            if execution_completed_at is not None:

                set_clauses.append(
                    f"execution_completed_at = "
                    f"TIMESTAMP('{execution_completed_at}')"
                )

            # -------------------------------------------------
            # Execution error
            # -------------------------------------------------

            if status in {"EXECUTION_REQUESTED", "RUNNING", "SUCCESS"}:
                set_clauses.append("execution_error = NULL")
            elif execution_error is not None:
                escaped_error = (str(execution_error).replace("'", "''"))
                set_clauses.append( f"execution_error = '{escaped_error}'")

            # -------------------------------------------------
            # Execute UPDATE
            # -------------------------------------------------

            update_sql = f"""
                UPDATE {self.backfill_request_table}

                SET
                    {", ".join(set_clauses)}

                WHERE request_id = '{request_id}'
            """

            self.spark.sql(update_sql)

            self.logger.info(
                "Backfill request status updated | "
                f"request_id={request_id} | "
                f"status={status}"
            )

        except Exception:

            self.logger.exception(
                "Failed to update backfill request status | "
                f"request_id={request_id} | "
                f"status={status}"
            )

            raise

    # =========================================================
    # Trigger Databricks job
    # =========================================================

    def trigger_databricks_job(
        self,
        parameters: dict
    ) -> int:
        """
        Trigger the Databricks backfill workflow
        using the Jobs REST API.
        """

        workspace_url = self.get_workspace_url()
        api_token = self.get_api_token()

        url = (
            f"{workspace_url}"
            f"/api/2.1/jobs/run-now"
        )

        headers = {
            "Authorization": f"Bearer {api_token}",
            "Content-Type": "application/json"
        }

        payload = {
            "job_id": self.JOB_ID,
            "job_parameters": parameters
        }

        self.logger.info(
            "Triggering Databricks backfill job | "
            f"job_name={self.JOB_NAME} | "
            f"job_id={self.JOB_ID} | "
            f"parameters={parameters}"
        )

        response = requests.post(
            url=url,
            headers=headers,
            json=payload,
            timeout=30
        )

        if response.status_code != 200:

            raise RuntimeError(
                "Databricks job trigger failed | "
                f"status_code={response.status_code} | "
                f"response={response.text}"
            )

        response_json = response.json()

        run_id = response_json.get("run_id")

        if run_id is None:

            raise RuntimeError(
                "Databricks job triggered but no run_id "
                "was returned."
            )

        self.logger.info(
            "Databricks backfill job triggered successfully | "
            f"run_id={run_id}"
        )

        return run_id

    # =========================================================
    # Execute backfill
    # =========================================================

    def execute_backfill(self,request_id: str):
        """
        Execute an approved backfill request.

        Lifecycle:

        APPROVED
            ↓
        EXECUTION_REQUESTED
            ↓
        RUNNING
            ↓
        SUCCESS / FAILED

        Validation and duplicate-request errors do NOT change the existing request status.
        """

        execution_started_at = datetime.now()

        try:

            self.logger.info(
            "Starting backfill execution | "
            f"request_id={request_id}"
            )

        # ---------------------------------------------------------
        # 1. Validate request
        # ---------------------------------------------------------

            request = self.validate_request_for_execution(
            request_id
            )

        # ---------------------------------------------------------
        # 2. Build Databricks job parameters
        # ---------------------------------------------------------

            parameters = self.build_job_parameters(
            request
        )

        # ---------------------------------------------------------
        # 3. Mark request as execution requested
        # ---------------------------------------------------------

            self.update_request_status(
            request_id=request_id,
            status="EXECUTION_REQUESTED",
            execution_started_at=execution_started_at
            )

        # ---------------------------------------------------------
        # 4. Trigger Databricks workflow
        # ---------------------------------------------------------

            run_id = self.trigger_databricks_job(
            parameters
            )

        # ---------------------------------------------------------
        # 5. Store Databricks execution ID
        # ---------------------------------------------------------

            self.update_request_status(
            request_id=request_id,
            status="RUNNING",
            execution_id=str(run_id),
            execution_started_at=execution_started_at
            )

        # ---------------------------------------------------------
        # 6. Monitor workflow
        # ---------------------------------------------------------

            final_status = self.wait_for_backfill_completion(
            request_id=request_id,
            databricks_run_id=run_id,
            poll_interval=15,
            timeout_minutes=30
            )

            self.logger.info(
            "Backfill execution completed | "
            f"request_id={request_id} | "
            f"run_id={run_id} | "
            f"status={final_status}"
            )

            return {
            "request_id": request_id,
            "run_id": run_id,
            "status": final_status
            }

        except ValueError as exc:

        # ---------------------------------------------------------
        # Validation / approval / idempotency rejection
        # ---------------------------------------------------------
        #
        # IMPORTANT:
        # Do NOT mark the request FAILED here.
        #
        # The request itself did not fail.
        # The execution request was rejected.
        # ---------------------------------------------------------

            self.logger.warning(
            "Backfill execution request rejected | "
            f"request_id={request_id} | "
            f"reason={str(exc)}"
            )

            raise

        except Exception as exc:

        # ---------------------------------------------------------
        # Actual execution/system failure
        # ---------------------------------------------------------

            error_message = str(exc)

            self.logger.exception(
            "Backfill execution failed | "
            f"request_id={request_id} | "
            f"error={error_message}"
            )

            try:

                self.update_request_status(
                request_id=request_id,
                status="FAILED",
                execution_completed_at=datetime.now(),
                execution_error=error_message
                )

            except Exception:

                self.logger.exception(
                "Failed to update backfill request "
                "status to FAILED | "
                f"request_id={request_id}"
                )

            raise

    # =========================================================
    # Get Databricks run status
    # =========================================================

    def get_run_status(
        self,
        run_id: int
    ):
        """
        Retrieve current status of a Databricks workflow run.
        """

        try:

            workspace_url = self.get_workspace_url()
            api_token = self.get_api_token()

            url = (
                f"{workspace_url}/api/2.1/jobs/runs/get"
                f"?run_id={run_id}"
            )

            response = requests.get(
                url,
                headers={
                    "Authorization": f"Bearer {api_token}"
                },
                timeout=30
            )

            response.raise_for_status()

            run_data = response.json()

            state = run_data.get(
                "state",
                {}
            )

            life_cycle_state = (
                state.get("life_cycle_state")
            )

            result_state = (
                state.get("result_state")
            )

            state_message = (
                state.get("state_message")
            )

            self.logger.info(
                "Databricks run status | "
                f"run_id={run_id} | "
                f"life_cycle_state={life_cycle_state} | "
                f"result_state={result_state} | "
                f"state_message={state_message}"
            )

            return {
                "life_cycle_state": life_cycle_state,
                "result_state": result_state,
                "state_message": state_message
            }

        except Exception:

            self.logger.exception(
                "Failed to retrieve Databricks run status | "
                f"run_id={run_id}"
            )

            raise

    # =========================================================
    # Resolve backfill status
    # =========================================================

    def resolve_backfill_status(
        self,
        run_status: dict
    ) -> str:
        """
        Convert Databricks workflow state into
        backfill request status.
        """

        life_cycle_state = run_status.get(
            "life_cycle_state"
        )

        result_state = run_status.get(
            "result_state"
        )

        if life_cycle_state in {
            "PENDING",
            "QUEUED",
            "RUNNING",
            "TERMINATING"
        }:

            return "RUNNING"

        if result_state == "SUCCESS":

            return "SUCCESS"

        if result_state in {
            "FAILED",
            "TIMEDOUT",
            "CANCELED"
        }:

            return "FAILED"

        return "RUNNING"

    # =========================================================
    # Wait for Databricks backfill completion
    # =========================================================

    def wait_for_backfill_completion(
        self,
        request_id: str,
        databricks_run_id: int,
        poll_interval: int = 15,
        timeout_minutes: int = 30
    ):
        """
        Poll Databricks until the workflow completes
        or monitoring times out.

        Returns:
            SUCCESS
            FAILED
            TIMEOUT
        """

        try:

            self.logger.info(
                "Starting backfill monitoring | "
                f"request_id={request_id} | "
                f"run_id={databricks_run_id}"
            )

            monitoring_start = datetime.now()

            timeout_seconds = (
                timeout_minutes * 60
            )

            while True:

                # =============================================
                # Check monitoring timeout
                # =============================================

                elapsed_seconds = (
                    datetime.now() - monitoring_start
                ).total_seconds()

                if elapsed_seconds >= timeout_seconds:

                    error_message = (
                        "Databricks backfill execution "
                        "monitoring timed out after "
                        f"{timeout_minutes} minutes."
                    )

                    self.logger.error(
                        f"{error_message} | "
                        f"request_id={request_id} | "
                        f"run_id={databricks_run_id}"
                    )

                    self.update_request_status(
                        request_id=request_id,
                        status="TIMEOUT",
                        execution_completed_at=datetime.now(),
                        execution_error=error_message
                    )

                    return "TIMEOUT"

                # =============================================
                # Get Databricks status
                # =============================================

                run_status = self.get_run_status(
                    databricks_run_id
                )

                life_cycle_state = (
                    run_status.get(
                        "life_cycle_state"
                    )
                )

                result_state = (
                    run_status.get(
                        "result_state"
                    )
                )

                state_message = (
                    run_status.get(
                        "state_message"
                    )
                )

                self.logger.info(
                    "Backfill execution status | "
                    f"request_id={request_id} | "
                    f"run_id={databricks_run_id} | "
                    f"life_cycle_state={life_cycle_state} | "
                    f"result_state={result_state}"
                )

                # =============================================
                # SUCCESS
                # =============================================

                if (
                    life_cycle_state == "TERMINATED"
                    and result_state == "SUCCESS"
                ):

                    completion_time = datetime.now()

                    self.update_request_status(
                        request_id=request_id,
                        status="SUCCESS",
                        execution_completed_at=completion_time
                    )

                    self.logger.info(
                        "Backfill execution completed successfully | "
                        f"request_id={request_id} | "
                        f"run_id={databricks_run_id}"
                    )

                    return "SUCCESS"

                # =============================================
                # FAILED / TIMEDOUT / CANCELED
                # =============================================

                if (
                    life_cycle_state == "TERMINATED"
                    and result_state in {
                        "FAILED",
                        "TIMEDOUT",
                        "CANCELED"
                    }
                ):

                    completion_time = datetime.now()

                    error_message = (
                        state_message
                        or (
                            "Databricks job ended with "
                            f"result_state={result_state}"
                        )
                    )

                    self.update_request_status(
                        request_id=request_id,
                        status="FAILED",
                        execution_completed_at=completion_time,
                        execution_error=error_message
                    )

                    self.logger.error(
                        "Backfill Databricks job failed | "
                        f"request_id={request_id} | "
                        f"run_id={databricks_run_id} | "
                        f"result_state={result_state}"
                    )

                    return "FAILED"

                # =============================================
                # INTERNAL ERROR
                # =============================================

                if life_cycle_state == "INTERNAL_ERROR":

                    completion_time = datetime.now()

                    error_message = (
                        state_message
                        or
                        "Databricks job entered "
                        "INTERNAL_ERROR state."
                    )

                    self.update_request_status(
                        request_id=request_id,
                        status="FAILED",
                        execution_completed_at=completion_time,
                        execution_error=error_message
                    )

                    return "FAILED"

                # =============================================
                # Still running
                # =============================================

                time.sleep(
                    poll_interval
                )

        except Exception:

            self.logger.exception(
                "Backfill monitoring failed | "
                f"request_id={request_id} | "
                f"run_id={databricks_run_id}"
            )

            raise
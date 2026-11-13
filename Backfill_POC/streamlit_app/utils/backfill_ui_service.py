import os
from datetime import datetime
from uuid import uuid4

import databricks.sql
from databricks.sdk import WorkspaceClient

from src.ai.backfill_agent_module import BackfillAgent
from src.backfill.backfill_request_module import BackfillRequest
from src.backfill.backfill_validator_module import BackfillValidator
from src.config.config_module import CONFIG
from src.utils.logging_utils_module import ApplicationLogger


class BackfillUIService:

    BACKFILL_REQUEST_TABLE = (
        "data_engineering.backfill_poc.backfill_request"
    )

    JOB_ID = 655023774126314
    JOB_NAME = "backfill_poc_pipeline"

    def __init__(self):

        self.logger = ApplicationLogger.get_logger(
            self.__class__.__name__
        )

        # =====================================================
        # OpenAI
        # =====================================================

        self.openai_api_key = os.getenv(
            "OPENAI_API_KEY"
        )

        if not self.openai_api_key:
            raise ValueError(
                "OPENAI_API_KEY environment variable is not configured."
            )

        self.agent = BackfillAgent(
            api_key=self.openai_api_key
        )

        self.validator = BackfillValidator()

        # =====================================================
        # Databricks App resources
        # =====================================================

        self.warehouse_id = os.getenv(
            "DATABRICKS_WAREHOUSE_ID"
        )

        self.databricks_host = os.getenv(
            "DATABRICKS_HOST"
        )

        self.client_id = os.getenv(
            "DATABRICKS_CLIENT_ID"
        )

        self.client_secret = os.getenv(
            "DATABRICKS_CLIENT_SECRET"
        )

        if not self.warehouse_id:
            raise ValueError(
                "DATABRICKS_WAREHOUSE_ID is not configured."
            )

        if not self.databricks_host:
            raise ValueError(
                "DATABRICKS_HOST is not configured."
            )

        if not self.client_id:
            raise ValueError(
                "DATABRICKS_CLIENT_ID is not configured."
            )

        if not self.client_secret:
            raise ValueError(
                "DATABRICKS_CLIENT_SECRET is not configured."
            )

        # Remove protocol if present
        self.server_hostname = (
            self.databricks_host
            .replace("https://", "")
            .replace("http://", "")
            .rstrip("/")
        )

        self.logger.info(
            "BackfillUIService initialized successfully."
        )

    # =========================================================
    # Databricks SQL Connection
    # =========================================================

    def _get_connection(self):

        http_path = (
            f"/sql/1.0/warehouses/{self.warehouse_id}"
        )

        def credential_provider():

            from databricks.sdk.core import Config
            from databricks.sdk.credentials_provider import (
                oauth_service_principal
            )

            config = Config(
                host=f"https://{self.server_hostname}",
                client_id=self.client_id,
                client_secret=self.client_secret
            )

            return oauth_service_principal(config)

        return databricks.sql.connect(
            server_hostname=self.server_hostname,
            http_path=http_path,
            credentials_provider=credential_provider
        )

    # =========================================================
    # Databricks Workspace Client
    # =========================================================

    def _get_workspace_client(self) -> WorkspaceClient:

        return WorkspaceClient(
            host=f"https://{self.server_hostname}",
            client_id=self.client_id,
            client_secret=self.client_secret
        )

    # =========================================================
    # AI Analysis
    # =========================================================

    def analyze_request(
        self,
        request_text: str
    ):

        self.logger.info(
            "Analyzing backfill request using AI."
        )

        return self.agent.analyze_request(
            request_text
        )

    # =========================================================
    # Submit Backfill Request
    # =========================================================

    def submit_request(
        self,
        request_text: str,
        requested_by: str
    ) -> BackfillRequest:

        try:

            if not request_text or not request_text.strip():

                raise ValueError(
                    "Backfill request cannot be empty."
                )

            self.logger.info(
                "Submitting backfill request."
            )

            # -------------------------------------------------
            # AI analysis
            # -------------------------------------------------

            analysis = self.agent.analyze_request(
                request_text
            )

            # -------------------------------------------------
            # Approval requirement
            # -------------------------------------------------

            approval_required = (
                self.validator
                .determine_approval_requirement(
                    analysis.risk_level
                )
            )

            # -------------------------------------------------
            # Validation
            # -------------------------------------------------

            self.validator.validate(
                start_date=analysis.start_date,
                end_date=analysis.end_date,
                risk_level=analysis.risk_level,
                date_ambiguous=analysis.date_ambiguous
            )

            # -------------------------------------------------
            # Date order
            # -------------------------------------------------

            if not analysis.date_order_valid:

                raise ValueError(
                    "Start date must be earlier than or equal "
                    "to end date."
                )

            # -------------------------------------------------
            # Request ID
            # -------------------------------------------------

            request_id = (
                f"REQ-"
                f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
                f"-{uuid4().hex[:6].upper()}"
            )

            now = datetime.now()

            request = BackfillRequest(
                request_id=request_id,
                requested_by=requested_by,
                request_text=request_text,
                business_unit=analysis.business_unit,
                start_date=analysis.start_date,
                end_date=analysis.end_date,
                risk_level=analysis.risk_level,
                risk_reason =analysis.risk_reason,
                impact_summary=analysis.impact_summary,
                proposed_strategy=analysis.proposed_strategy,
                approval_required=approval_required,
                status="PENDING",
                created_at=now,
                updated_at=now
            )

            # -------------------------------------------------
            # Persist
            # -------------------------------------------------

            insert_sql = f"""
                INSERT INTO {self.BACKFILL_REQUEST_TABLE}
                (
                    request_id,
                    requested_by,
                    request_text,
                    business_unit,
                    start_date,
                    end_date,
                    status,
                    risk_level,
                    risk_reason,
                    impact_summary,
                    proposed_strategy,
                    approval_required,
                    approved_by,
                    approved_at,
                    rejected_by,
                    rejected_at,
                    rejection_reason,
                    created_at,
                    updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
            """

            with self._get_connection() as connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        insert_sql,
                        (
                            request.request_id,
                            request.requested_by,
                            request.request_text,
                            request.business_unit,
                            request.start_date,
                            request.end_date,
                            request.status,
                            request.risk_level,
                            request.risk_reason,
                            request.impact_summary,
                            request.proposed_strategy,
                            request.approval_required,
                            request.approved_by,
                            request.approved_at,
                            request.rejected_by,
                            request.rejected_at,
                            request.rejection_reason,
                            request.created_at,
                            request.updated_at
                        )
                    )

                connection.commit()

            self.logger.info(
                "Backfill request submitted successfully | "
                f"request_id={request_id}"
            )

            return request

        except Exception:

            self.logger.exception(
                "Failed to submit backfill request."
            )

            raise

    # =========================================================
    # Get Requests
    # =========================================================

    def get_requests(
        self,
        status: str | None = None
    ):

        try:

            sql = f"""
                SELECT
                    request_id,
                    requested_by,
                    request_text,
                    business_unit,
                    start_date,
                    end_date,
                    status,
                    risk_level,
                    risk_reason,
                    impact_summary,
                    proposed_strategy,
                    approval_required,
                    approved_by,
                    approved_at,
                    rejected_by,
                    rejected_at,
                    rejection_reason,
                    created_at,
                    updated_at,
                    execution_id,
                    execution_started_at,
                    execution_completed_at,
                    execution_error
                FROM {self.BACKFILL_REQUEST_TABLE}
            """

            params = ()

            if status:

                sql += """
                    WHERE status = ?
                """

                params = (status,)

            sql += """
                ORDER BY created_at DESC
            """

            with self._get_connection() as connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        sql,
                        params
                    )

                    rows = cursor.fetchall()

                    columns = [
                        description[0]
                        for description in cursor.description
                    ]

            return [
                dict(zip(columns, row))
                for row in rows
            ]

        except Exception:

            self.logger.exception(
                "Failed to retrieve backfill requests."
            )

            raise

    # =========================================================
    # Get Single Request
    # =========================================================

    def get_request(
        self,
        request_id: str
    ):

        requests = self.get_requests()

        for request in requests:

            if request["request_id"] == request_id:
                return request

        return None

    # =========================================================
    # Approve Request
    # =========================================================

    def approve_request(
        self,
        request_id: str,
        approved_by: str
    ):

        try:

            request = self.get_request(
                request_id
            )

            if request is None:

                raise ValueError(
                    f"Request not found | "
                    f"request_id={request_id}"
                )

            if request["status"] != "PENDING":

                raise ValueError(
                    f"Request cannot be approved because "
                    f"current status is "
                    f"{request['status']}."
                )

            if not request["approval_required"]:

                raise ValueError(
                    "Approval is not required for this request."
                )

            now = datetime.now()

            update_sql = f"""
                UPDATE {self.BACKFILL_REQUEST_TABLE}
                SET
                    status = ?,
                    approved_by = ?,
                    approved_at = ?,
                    updated_at = ?
                WHERE request_id = ?
            """

            with self._get_connection() as connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        update_sql,
                        (
                            "APPROVED",
                            approved_by,
                            now,
                            now,
                            request_id
                        )
                    )

                connection.commit()

            self.logger.info(
                "Backfill request approved | "
                f"request_id={request_id} | "
                f"approved_by={approved_by}"
            )

        except Exception:

            self.logger.exception(
                "Failed to approve backfill request | "
                f"request_id={request_id}"
            )

            raise

    # =========================================================
    # Reject Request
    # =========================================================

    def reject_request(
        self,
        request_id: str,
        rejected_by: str,
        rejection_reason: str
    ):

        try:

            request = self.get_request(
                request_id
            )

            if request is None:

                raise ValueError(
                    f"Request not found | "
                    f"request_id={request_id}"
                )

            if request["status"] != "PENDING":

                raise ValueError(
                    f"Request cannot be rejected because "
                    f"current status is "
                    f"{request['status']}."
                )

            now = datetime.now()

            update_sql = f"""
                UPDATE {self.BACKFILL_REQUEST_TABLE}
                SET
                    status = ?,
                    rejected_by = ?,
                    rejected_at = ?,
                    rejection_reason = ?,
                    updated_at = ?
                WHERE request_id = ?
            """

            with self._get_connection() as connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        update_sql,
                        (
                            "REJECTED",
                            rejected_by,
                            now,
                            rejection_reason,
                            now,
                            request_id
                        )
                    )

                connection.commit()

            self.logger.info(
                "Backfill request rejected | "
                f"request_id={request_id}"
            )

        except Exception:

            self.logger.exception(
                "Failed to reject backfill request | "
                f"request_id={request_id}"
            )

            raise

    # =========================================================
    # Execute Backfill - NON BLOCKING
    # =========================================================

    def execute_backfill(
        self,
        request_id: str
    ) -> dict:

        """
        Trigger the Databricks backfill job.

        IMPORTANT:
        This method does NOT wait for job completion.

        Lifecycle:

            APPROVED
                ↓
            EXECUTION_REQUESTED
                ↓
            Databricks Job Triggered
                ↓
            RUNNING

        The UI can later call get_execution_status().
        """

        try:

            self.logger.info(
                "Starting backfill execution request | "
                f"request_id={request_id}"
            )

            # -------------------------------------------------
            # Get request
            # -------------------------------------------------

            request = self.get_request(
                request_id
            )

            if request is None:

                raise ValueError(
                    f"Backfill request not found | "
                    f"request_id={request_id}"
                )

            # -------------------------------------------------
            # Validate status
            # -------------------------------------------------

            if request["status"] != "APPROVED":

                raise ValueError(
                    f"Backfill request is not APPROVED | "
                    f"request_id={request_id} | "
                    f"status={request['status']}"
                )

            # -------------------------------------------------
            # Idempotency
            # -------------------------------------------------

            if request.get("execution_id"):

                raise ValueError(
                    f"Backfill execution already exists | "
                    f"request_id={request_id} | "
                    f"execution_id={request['execution_id']}"
                )

            # -------------------------------------------------
            # Build parameters
            # -------------------------------------------------

            parameters = {
                "mode": "BACKFILL",
                "start_date": str(
                    request["start_date"]
                ),
                "end_date": str(
                    request["end_date"]
                ),
                "request_id": str(
                    request["request_id"]
                )
            }

            self.logger.info(
                "Backfill job parameters created | "
                f"request_id={request_id} | "
                f"parameters={parameters}"
            )

            # -------------------------------------------------
            # Mark execution requested
            # -------------------------------------------------

            execution_started_at = datetime.now()

            update_sql = f"""
                UPDATE {self.BACKFILL_REQUEST_TABLE}
                SET
                    status = ?,
                    execution_started_at = ?,
                    updated_at = ?
                WHERE request_id = ?
                  AND status = 'APPROVED'
            """

            with self._get_connection() as connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        update_sql,
                        (
                            "EXECUTION_REQUESTED",
                            execution_started_at,
                            execution_started_at,
                            request_id
                        )
                    )

                connection.commit()

            # -------------------------------------------------
            # Trigger Databricks job
            # -------------------------------------------------

            workspace = self._get_workspace_client()

            run_response = workspace.jobs.run_now(
                job_id=self.JOB_ID,
                job_parameters=parameters
            )

            run_id = run_response.run_id

            if run_id is None:

                raise RuntimeError(
                    "Databricks job was triggered but "
                    "no run_id was returned."
                )

            # -------------------------------------------------
            # Store execution ID
            # -------------------------------------------------

            update_sql = f"""
                UPDATE {self.BACKFILL_REQUEST_TABLE}
                SET
                    status = ?,
                    execution_id = ?,
                    updated_at = ?
                WHERE request_id = ?
            """

            with self._get_connection() as connection:

                with connection.cursor() as cursor:

                    cursor.execute(
                        update_sql,
                        (
                            "RUNNING",
                            str(run_id),
                            datetime.now(),
                            request_id
                        )
                    )

                connection.commit()

            self.logger.info(
                "Backfill Databricks job triggered successfully | "
                f"request_id={request_id} | "
                f"run_id={run_id}"
            )

            return {
                "request_id": request_id,
                "run_id": run_id,
                "status": "RUNNING"
            }

        except Exception as exc:

            self.logger.exception(
                "Backfill execution failed | "
                f"request_id={request_id}"
            )

            # -------------------------------------------------
            # Attempt to mark request FAILED
            # -------------------------------------------------

            try:

                update_sql = f"""
                    UPDATE {self.BACKFILL_REQUEST_TABLE}
                    SET
                        status = ?,
                        execution_completed_at = ?,
                        execution_error = ?,
                        updated_at = ?
                    WHERE request_id = ?
                      AND status = 'EXECUTION_REQUESTED'
                """

                now = datetime.now()

                with self._get_connection() as connection:

                    with connection.cursor() as cursor:

                        cursor.execute(
                            update_sql,
                            (
                                "FAILED",
                                now,
                                str(exc),
                                now,
                                request_id
                            )
                        )

                    connection.commit()

            except Exception:

                self.logger.exception(
                    "Failed to update execution failure status | "
                    f"request_id={request_id}"
                )

            raise

    # =========================================================
    # Get Databricks Execution Status
    # =========================================================

    def get_execution_status(
        self,
        request_id: str
    ) -> dict:

        try:

            request = self.get_request(
                request_id
            )

            if request is None:

                raise ValueError(
                    f"Request not found | "
                    f"request_id={request_id}"
                )

            execution_id = request.get(
                "execution_id"
            )

            if not execution_id:

                return {
                    "request_id": request_id,
                    "status": request["status"],
                    "execution_id": None,
                    "life_cycle_state": None,
                    "result_state": None,
                    "state_message": None
                }

            workspace = self._get_workspace_client()

            run = workspace.jobs.get_run(
                run_id=int(execution_id)
            )

            state = run.state

            life_cycle_state = (
                state.life_cycle_state.value
                if state.life_cycle_state
                else None
            )

            result_state = (
                state.result_state.value
                if state.result_state
                else None
            )

            state_message = (
                state.state_message
                if state.state_message
                else None
            )

            # -------------------------------------------------
            # Resolve application status
            # -------------------------------------------------

            if life_cycle_state in {
                "PENDING",
                "QUEUED",
                "RUNNING",
                "TERMINATING"
            }:

                application_status = "RUNNING"

            elif result_state == "SUCCESS":

                application_status = "SUCCESS"

            elif result_state in {
                "FAILED",
                "TIMEDOUT",
                "CANCELED"
            }:

                application_status = "FAILED"

            else:

                application_status = request["status"]

            # -------------------------------------------------
            # Update DB status
            # -------------------------------------------------

            if application_status != request["status"]:

                now = datetime.now()

                if application_status == "SUCCESS":

                    update_sql = f"""
                        UPDATE {self.BACKFILL_REQUEST_TABLE}
                        SET
                            status = ?,
                            execution_completed_at = ?,
                            execution_error = NULL,
                            updated_at = ?
                        WHERE request_id = ?
                    """

                    params = (
                        "SUCCESS",
                        now,
                        now,
                        request_id
                    )

                elif application_status == "FAILED":

                    error_message = (
                        state_message
                        or
                        f"Databricks run ended with "
                        f"result_state={result_state}"
                    )

                    update_sql = f"""
                        UPDATE {self.BACKFILL_REQUEST_TABLE}
                        SET
                            status = ?,
                            execution_completed_at = ?,
                            execution_error = ?,
                            updated_at = ?
                        WHERE request_id = ?
                    """

                    params = (
                        "FAILED",
                        now,
                        error_message,
                        now,
                        request_id
                    )

                else:

                    update_sql = f"""
                        UPDATE {self.BACKFILL_REQUEST_TABLE}
                        SET
                            status = ?,
                            updated_at = ?
                        WHERE request_id = ?
                    """

                    params = (
                        application_status,
                        now,
                        request_id
                    )

                with self._get_connection() as connection:

                    with connection.cursor() as cursor:

                        cursor.execute(
                            update_sql,
                            params
                        )

                    connection.commit()

            return {
                "request_id": request_id,
                "status": application_status,
                "execution_id": execution_id,
                "life_cycle_state": life_cycle_state,
                "result_state": result_state,
                "state_message": state_message,
                "execution_started_at": request.get(
                    "execution_started_at"
                ),
                "execution_completed_at": request.get(
                    "execution_completed_at"
                ),
                "execution_error": (
                    state_message
                    if application_status == "FAILED"
                    else request.get("execution_error")
                )
            }

        except Exception:

            self.logger.exception(
                "Failed to retrieve execution status | "
                f"request_id={request_id}"
            )

            raise
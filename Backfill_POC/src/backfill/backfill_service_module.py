from datetime import datetime
from uuid import uuid4

from pyspark.sql.types import StructType, StructField, StringType, DateType, BooleanType, TimestampType

from src.config.config_module import CONFIG
from src.backfill.backfill_request_module import BackfillRequest
from src.backfill.backfill_validator_module import BackfillValidator
from src.utils.logging_utils_module import ApplicationLogger
from src.ai.backfill_agent_module import BackfillAgent


class BackfillService:

    def __init__(self, spark, api_key):

        self.spark = spark

        self.logger = ApplicationLogger.get_logger(
            self.__class__.__name__
        )

        self.validator = BackfillValidator()

        self.agent = BackfillAgent(api_key=api_key)

        self.backfill_request_table = CONFIG.tables.backfill_request

    def generate_request_id(self) -> str:
        """
        Generate a unique identifier for a backfill request.
        """

        return f"REQ-{datetime.now().strftime('%Y%m%d%H%M%S')}-{uuid4().hex[:6].upper()}"

    def validate_request(self,start_date,end_date,risk_level) -> None:
        """
        Validate backfill request using deterministic rules.
        """

        self.validator.validate(
            start_date=start_date,
            end_date=end_date,
            risk_level=risk_level
        )

        self.logger.info(
            "Backfill request validation successful | "
            f"start_date={start_date} | "
            f"end_date={end_date} | "
            f"risk_level={risk_level}"
        )

    def create_request(self,requested_by: str,request_text: str,business_unit: str,start_date,end_date,risk_level: str,impact_summary: str,proposed_strategy: str,
                        approval_required: bool) -> BackfillRequest:
        """
        Create a validated BackfillRequest object.
        """

        try:

        # --------------------------------------------------
        # 1. Validate request
        # --------------------------------------------------

            self.validate_request(
                start_date=start_date,
                end_date=end_date,
                risk_level=risk_level
            )

        # --------------------------------------------------
        # 2. Generate request ID
        # --------------------------------------------------

            request_id = self.generate_request_id()

        # --------------------------------------------------
        # 3. Create request object
        # --------------------------------------------------

            request_time = datetime.now()

            request = BackfillRequest(
                request_id=request_id,
                requested_by=requested_by,
                request_text=request_text,
                business_unit=business_unit,
                start_date=start_date,
                end_date=end_date,
                risk_level=risk_level,
                impact_summary=impact_summary,
                proposed_strategy=proposed_strategy,
                approval_required=approval_required,
                status="PENDING",
                created_at=request_time,
                updated_at=request_time
            )

        # --------------------------------------------------
        # 4. Persist request
        # --------------------------------------------------

            data = [(
                request.request_id,
                request.requested_by,
                request.request_text,
                request.business_unit,
                request.start_date,
                request.end_date,
                request.status,
                request.risk_level,
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
                )]

            columns = StructType([
                StructField("request_id", StringType(), True),
                StructField("requested_by", StringType(), True),
                StructField("request_text", StringType(), True),
                StructField("business_unit", StringType(), True),
                StructField("start_date", DateType(), True),
                StructField("end_date", DateType(), True),
                StructField("status", StringType(), True),
                StructField("risk_level", StringType(), True),
                StructField("impact_summary", StringType(), True),
                StructField("proposed_strategy", StringType(), True),
                StructField("approval_required", BooleanType(), True),
                StructField("approved_by", StringType(), True),
                StructField("approved_at", TimestampType(), True),
                StructField("rejected_by", StringType(), True),
                StructField("rejected_at", TimestampType(), True),
                StructField("rejection_reason", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("updated_at", TimestampType(), True)
                ])

            request_df = self.spark.createDataFrame(
                data=data,
                schema=columns
            )

            (
            request_df.write.format("delta").mode("append").saveAsTable(self.backfill_request_table)
            )

        # --------------------------------------------------
        # 5. Log successful persistence
        # --------------------------------------------------

            self.logger.info("Backfill request persisted successfully | "f"request_id={request_id}")

            return request

        except Exception:

            self.logger.exception("Failed to create and persist backfill request.")

            raise

    def save_request(self,request: BackfillRequest) -> None:
        """
        Persist backfill request into Delta control table.
        """

        try:

            current_time = datetime.now()

            data = [(
                request.request_id,
                request.requested_by,
                request.request_text,
                request.business_unit,
                request.start_date,
                request.end_date,
                "VALIDATED",
                request.risk_level,
                request.impact_summary,
                request.proposed_strategy,
                request.approval_required,
                None,
                None,
                None,
                None,
                None,
                current_time,
                current_time
            )]

            schema = StructType([
                StructField("request_id", StringType(), False),
                StructField("requested_by", StringType(), False),
                StructField("request_text", StringType(), False),
                StructField("business_unit", StringType(), True),
                StructField("start_date", DateType(), False),
                StructField("end_date", DateType(), False),
                StructField("status", StringType(), False),
                StructField("risk_level", StringType(), True),
                StructField("impact_summary", StringType(), True),
                StructField("proposed_strategy", StringType(), True),
                StructField("approval_required", BooleanType(), False),
                StructField("approved_by", StringType(), True),
                StructField("approved_at", TimestampType(), True),
                StructField("rejected_by", StringType(), True),
                StructField("rejected_at", TimestampType(), True),
                StructField("rejection_reason", StringType(), True),
                StructField("created_at", TimestampType(), False),
                StructField("updated_at", TimestampType(), False)
            ])

            request_df = self.spark.createDataFrame(
                data,
                schema
            )

            (
                request_df.write
                .format("delta")
                .mode("append")
                .saveAsTable(
                    "data_engineering.backfill_poc.backfill_request"
                )
            )

            self.logger.info(
                "Backfill request persisted successfully | "
                f"request_id={request.request_id}"
            )

        except Exception:

            self.logger.exception(
                "Failed to persist backfill request | "
                f"request_id={request.request_id}"
            )

            raise
    
    def submit_request(
        self,
        requested_by: str,
        request_text: str,
        business_unit: str,
        start_date,
        end_date,
        risk_level: str,
        impact_summary: str,
        proposed_strategy: str,
        approval_required: bool
    ) -> BackfillRequest:
        """
        Validate, create and persist a backfill request.
        """

        request = self.create_request(
            requested_by=requested_by,
            request_text=request_text,
            business_unit=business_unit,
            start_date=start_date,
            end_date=end_date,
            risk_level=risk_level,
            impact_summary=impact_summary,
            proposed_strategy=proposed_strategy,
            approval_required=approval_required
        )

        self.save_request(request)

        return request  

    def create_request_from_text(self,request_text: str,requested_by: str):
        """
        Analyze a natural-language backfill request using AI,
        validate the resulting proposal and persist the request.
        """

        try:

            self.logger.info("Starting natural-language backfill request processing.")


            analysis = self.agent.analyze_request(request_text)

            self.logger.info(
                f"AI analysis completed | "
                f"business_unit={analysis.business_unit} | "
                f"risk_level={analysis.risk_level} | "
                f"date_ambiguous={analysis.date_ambiguous}"
            )


            approval_required = (
                self.validator.determine_approval_requirement(analysis.risk_level)
            )


            self.validator.validate(
                start_date=analysis.start_date,
                end_date=analysis.end_date,
                risk_level=analysis.risk_level,
                date_ambiguous=analysis.date_ambiguous
            )

            request = self.create_request(
                requested_by=requested_by,
                request_text=request_text,
                business_unit=analysis.business_unit,
                start_date=analysis.start_date,
                end_date=analysis.end_date,
                risk_level=analysis.risk_level,
                impact_summary=analysis.impact_summary,
                proposed_strategy=analysis.proposed_strategy,
                approval_required=approval_required
            )

            self.logger.info(
                f"Natural-language backfill request created | "
                f"request_id={request.request_id}"
            )

            return request

        except Exception:

            self.logger.exception("Failed to process natural-language backfill request.")

            raise         

    def approve_request(self,request_id: str,approved_by: str):
        """
        Approve a pending backfill request.
        Only PENDING requests requiring approval can be approved.
        """

        try:

            self.logger.info(
                f"Starting approval | request_id={request_id} | "
                f"approved_by={approved_by}"
            )

            request_df = (
                self.spark.table(self.backfill_request_table).filter(f"request_id = '{request_id}'")
            )

            requests = request_df.collect()

            if not requests:
                raise ValueError(
                    f"Backfill request not found | "
                    f"request_id={request_id}"
                )

            request = requests[0]

            if request.status != "PENDING":
                raise ValueError(
                    f"Request cannot be approved because its "
                    f"current status is {request.status} | "
                    f"request_id={request_id}"
                )

            if not request.approval_required:
                raise ValueError(
                    f"Approval is not required for request | "
                    f"request_id={request_id}"
                )

            approval_time = datetime.now()

            (
            self.spark.sql(
                f"""
                UPDATE {self.backfill_request_table}
                SET
                    status = 'APPROVED',
                    approved_by = '{approved_by}',
                    approved_at = TIMESTAMP('{approval_time}'),
                    updated_at = TIMESTAMP('{approval_time}')
                WHERE request_id = '{request_id}'
                """
                )
            )

            self.logger.info(
            f"Backfill request approved successfully | "
            f"request_id={request_id} | "
            f"approved_by={approved_by}"
            )

            return True

        except Exception:

            self.logger.exception(
            f"Failed to approve backfill request | "
            f"request_id={request_id}"
            )

            raise

    def reject_request(self,request_id: str,rejected_by: str,rejection_reason: str):
        """
        Reject a pending backfill request.
        """
        try:

            self.logger.info(
                f"Starting rejection | request_id={request_id} | "
                f"rejected_by={rejected_by}"
            )

            request_df = (
                self.spark.table(self.backfill_request_table).filter(f"request_id = '{request_id}'"))

            requests = request_df.collect()

            if not requests:
                raise ValueError(
                    f"Backfill request not found | "
                    f"request_id={request_id}"
                )

            request = requests[0]

            if request.status != "PENDING":
                raise ValueError(
                f"Request cannot be rejected because its "
                f"current status is {request.status} | "
                f"request_id={request_id}"
                )

            rejection_time = datetime.now()

            escaped_reason = rejection_reason.replace("'","''")

            (
            self.spark.sql(
                f"""
                UPDATE {self.backfill_request_table}
                SET
                    status = 'REJECTED',
                    rejected_by = '{rejected_by}',
                    rejected_at  = TIMESTAMP('{rejection_time}'),
                    rejection_reason = '{escaped_reason}',
                    updated_at = TIMESTAMP('{rejection_time}')
                WHERE request_id = '{request_id}'
                """
                )
            )

            self.logger.info(
            f"Backfill request rejected successfully | "
            f"request_id={request_id} | "
            f"rejected_by={rejected_by}"
            )

            return True

        except Exception:

            self.logger.exception(
            f"Failed to reject backfill request | "
            f"request_id={request_id}")

            raise
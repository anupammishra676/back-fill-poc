from datetime import date

from pydantic import BaseModel, Field
from openai import OpenAI

from src.utils.logging_utils_module import ApplicationLogger


class BackfillAnalysis(BaseModel):

    business_unit: str = Field(
        description="Business unit affected by the backfill."
    )

    start_date: date | None = Field(
        description=(
            "Start date of the requested backfill. "
            "Return null if the date cannot be determined reliably."
        )
    )

    end_date: date | None = Field(
        description=(
            "End date of the requested backfill. "
            "Return null if the date cannot be determined reliably."
        )
    )

    date_ambiguous: bool = Field(
        description=(
            "True if the request does not provide enough "
            "information to determine the dates reliably."
        )
    )

    date_order_valid: bool = Field(
        description=(
            "True only if the start date is earlier than or equal "
            "to the end date exactly as stated by the user. "
            "Never reorder or correct the dates."
        )
    )

    risk_level: str = Field(
        description=(
            "Risk level of the requested backfill. "
            "Must be LOW, MEDIUM or HIGH."
        )
    )

    risk_reason: str = Field(
        description=(
            "Clear explanation of why the request was classified "
            "as LOW, MEDIUM or HIGH risk. "
            "Mention the specific factors from the request that "
            "contributed to the risk assessment."
        )
    )

    impact_summary: str = Field(
        description=(
            "Business and data impact of performing the backfill."
        )
    )

    proposed_strategy: str = Field(
        description=(
            "Recommended strategy for processing the backfill."
        )
    )


class BackfillAgent:

    def __init__(self, api_key: str):

        self.client = OpenAI(
            api_key=api_key
        )

        self.logger = ApplicationLogger.get_logger(
            self.__class__.__name__
        )

    def analyze_request(
        self,
        request_text: str
    ) -> BackfillAnalysis:

        try:

            if not request_text or not request_text.strip():

                raise ValueError(
                    "Backfill request text cannot be empty."
                )

            self.logger.info(
                "Starting AI analysis of backfill request."
            )

            response = self.client.responses.parse(

                model="gpt-4.1-mini",

                input=[

                    {
                        "role": "system",

                        "content": (
                            "You are an enterprise data engineering "
                            "backfill assistant.\n\n"

                            "Analyze the user's backfill request and "
                            "extract the required information.\n\n"

                            "Rules:\n"

                            "1. Do not execute any pipeline.\n"

                            "2. Do not trigger any external system.\n"

                            "3. Do not invent missing information.\n"

                            "4. risk_level must be LOW, MEDIUM or HIGH.\n"

                            "5. Extract the requested start and end dates.\n"

                            "6. If the year is not explicitly provided "
                            "and cannot be determined reliably from "
                            "the request, set date_ambiguous=true and "
                            "set start_date and end_date to null.\n"

                            "7. If either date is ambiguous, do not "
                            "guess the date.\n"

                            "8. proposed_strategy should describe the "
                            "recommended processing approach.\n"

                            "9. Preserve the user's date order exactly "
                            "as stated.\n"

                            "10. Never swap, reorder, correct or "
                            "reinterpret start_date and end_date.\n"

                            "11. If the user provides a start date "
                            "later than the end date, preserve both "
                            "dates exactly and set "
                            "date_order_valid=false.\n"

                            "12. The AI extracts the request; it must "
                            "not correct invalid user input.\n\n"

                            "13. Assess the backfill risk using factors "
                            "such as:\n"
                            "   - business criticality\n"
                            "   - size of the requested date range\n"
                            "   - historical versus recent processing\n"
                            "   - potential downstream impact\n"
                            "   - financial or operational sensitivity\n"
                            "   - likelihood of affecting downstream "
                            "aggregations or reports\n\n"

                            "14. risk_reason must explain the actual "
                            "factors that caused the selected risk "
                            "level. Do not provide generic statements.\n\n"

                            "15. Do not treat LOW, MEDIUM or HIGH as "
                            "approval decisions. Risk assessment and "
                            "approval requirement are separate concepts.\n"
                        )
                    },

                    {
                        "role": "user",
                        "content": request_text
                    }
                ],

                text_format=BackfillAnalysis
            )

            analysis = response.output_parsed

            if analysis is None:

                raise ValueError(
                    "AI did not return a valid backfill analysis."
                )

            self.logger.info(
                "AI backfill analysis completed successfully | "
                f"risk_level={analysis.risk_level}"
            )

            return analysis

        except Exception:

            self.logger.exception(
                "Failed to analyze backfill request using AI."
            )

            raise
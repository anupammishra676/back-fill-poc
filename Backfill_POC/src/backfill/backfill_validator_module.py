from datetime import date


class BackfillValidator:

    ALLOWED_RISK_LEVELS = {
        "LOW",
        "MEDIUM",
        "HIGH"
    }

    def validate(self,start_date,end_date,risk_level,date_ambiguous=False) -> None:

        if date_ambiguous:
            raise ValueError(
            "Backfill request contains ambiguous dates."
        )

        if not start_date:
            raise ValueError(
            "Backfill start_date is required."
        )

        if not end_date:
            raise ValueError(
            "Backfill end_date is required."
        )

        if start_date > end_date:
            raise ValueError(
            "start_date cannot be greater than end_date."
        )

        if risk_level not in self.ALLOWED_RISK_LEVELS:
            raise ValueError(
            f"Invalid risk level: {risk_level}"
        )
    
    def determine_approval_requirement(self,risk_level: str) -> bool:
        """
        Determine whether human approval is required
        based on deterministic governance rules.
        """

        return risk_level in {"MEDIUM","HIGH"}
        
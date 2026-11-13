from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional


@dataclass
class BackfillRequest:

    request_id: str
    requested_by: str
    request_text: str
    business_unit: str

    start_date: date
    end_date: date

    risk_level: str
    risk_reason: str

    impact_summary: str
    proposed_strategy: str

    approval_required: bool

    # Request status
    status: str = "PENDING"

    # Approval information
    approved_by: Optional[str] = None
    approved_at: Optional[datetime] = None

    # Rejection information
    rejected_by: Optional[str] = None
    rejected_at: Optional[datetime] = None
    rejection_reason: Optional[str] = None

    # Audit timestamps
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
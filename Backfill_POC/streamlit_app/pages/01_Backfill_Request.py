import sys
from pathlib import Path

import streamlit as st


# ============================================================
# Project Path Setup
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# Application Imports
# ============================================================

from streamlit_app.utils.backfill_ui_service import BackfillUIService



# ============================================================
# Page Configuration
# ============================================================

st.set_page_config(
    page_title="Backfill Request",
    page_icon="🔄",
    layout="wide"
)


# ============================================================
# Session State
# ============================================================

if "backfill_analysis" not in st.session_state:
    st.session_state.backfill_analysis = None

if "submitted_request_id" not in st.session_state:
    st.session_state.submitted_request_id = None

if "analysis_request_text" not in st.session_state:
    st.session_state.analysis_request_text = None


# ============================================================
# Header
# ============================================================

st.title("🔄 Backfill Request")

st.markdown(
    """
    Submit a backfill request using natural language.

    The AI assistant will analyze the request and generate a structured
    proposal. You can review the proposal before submitting it for
    validation and approval.
    """
)

st.divider()


# ============================================================
# Initialize Service
# ============================================================

@st.cache_resource
def get_backfill_service():
    return BackfillUIService()


try:
    service = get_backfill_service()

except Exception as exc:
    st.error(
        "Unable to initialize the Backfill service."
    )

    st.exception(exc)

    st.stop()


# ============================================================
# Request Details
# ============================================================

st.subheader("1. Request Details")

col1, col2 = st.columns([2, 1])

with col1:

    request_text = st.text_area(
        "Backfill Request",
        placeholder=(
            "Example:\n"
            "Please backfill ecommerce data for August 20 to August 22, "
            "2026 because of a finance correction."
        ),
        height=140,
        help=(
            "Describe the business reason and date range for the "
            "backfill request."
        )
    )

with col2:

    requested_by = st.text_input(
        "Requested By",
        placeholder="Enter your name",
        help="Name of the person requesting the backfill."
    )


# ============================================================
# Analyze Request
# ============================================================

st.markdown("###")

analyze_clicked = st.button(
    "🤖 Analyze Request",
    type="primary",
    use_container_width=True
)


if analyze_clicked:

    # Clear previous submission
    st.session_state.submitted_request_id = None

    # Basic UI validation
    if not request_text or not request_text.strip():

        st.warning(
            "Please enter a backfill request before analyzing."
        )

        st.stop()

    if not requested_by or not requested_by.strip():

        st.warning(
            "Please enter the requester name."
        )

        st.stop()

    try:

        with st.spinner(
            "Analyzing request using AI..."
        ):

            analysis = service.analyze_request(
                request_text=request_text.strip()
            )

        st.session_state.backfill_analysis = analysis
        st.session_state.analysis_request_text = request_text.strip()

        st.success(
            "AI analysis completed successfully."
        )

    except Exception as exc:

        st.session_state.backfill_analysis = None

        st.error(
            "Unable to analyze the backfill request."
        )

        st.exception(exc)


# ============================================================
# AI Analysis Result
# ============================================================

analysis = st.session_state.backfill_analysis


if analysis is not None:

    st.divider()

    st.subheader("2. AI Analysis")

    st.caption(
        "Review the AI-generated proposal before submitting the request."
    )

    # --------------------------------------------------------
    # Summary Metrics
    # --------------------------------------------------------

    metric_col1, metric_col2, metric_col3 = st.columns(3)

    with metric_col1:

        st.metric(
            "Business Unit",
            analysis.business_unit
        )

    with metric_col2:

        st.metric(
            "Risk Level",
            analysis.risk_level
        )

    with metric_col3:

        if analysis.date_ambiguous:
            date_status = "Ambiguous"
        elif not analysis.date_order_valid:
            date_status = "Invalid Range"
        else:
            date_status = "Valid"

        st.metric(
            "Date Status",
            date_status
        )

    # --------------------------------------------------------
    # Date Information
    # --------------------------------------------------------

    st.markdown("#### Backfill Window")

    date_col1, date_col2 = st.columns(2)

    with date_col1:

        st.text_input(
            "Start Date",
            value=(
                str(analysis.start_date)
                if analysis.start_date
                else "Not determined"
            ),
            disabled=True
        )

    with date_col2:

        st.text_input(
            "End Date",
            value=(
                str(analysis.end_date)
                if analysis.end_date
                else "Not determined"
            ),
            disabled=True
        )

    # --------------------------------------------------------
    # Impact Analysis
    # --------------------------------------------------------

    st.markdown("#### Business & Data Impact")

    st.info(
        analysis.impact_summary
    )

    # --------------------------------------------------------
    # Proposed Strategy
    # --------------------------------------------------------

    st.markdown("#### Proposed Processing Strategy")

    st.success(
        analysis.proposed_strategy
    )

    # --------------------------------------------------------
    # Validation Warnings
    # --------------------------------------------------------

    validation_error = None

    if analysis.date_ambiguous:

        validation_error = (
            "The requested date range could not be determined reliably."
        )

    elif not analysis.date_order_valid:

        validation_error = (
            "The start date is later than the end date. "
            "The request cannot be submitted."
        )

    elif analysis.start_date is None or analysis.end_date is None:

        validation_error = (
            "A valid start date and end date are required."
        )

    if validation_error:

        st.warning(
            f"⚠️ {validation_error}"
        )

        st.caption(
            "Please correct the request and run the AI analysis again."
        )

    # ========================================================
    # Submit Request
    # ========================================================

    st.markdown("###")

    st.subheader("3. Submit Request")

    approval_required = False

    try:

        approval_required = (
            service.validator.determine_approval_requirement(
                analysis.risk_level
            )
        )

    except Exception:

        approval_required = False

    if approval_required:

        st.warning(
            "⚠️ Approval will be required before this backfill can execute."
        )

    else:

        st.info(
            "ℹ️ This request does not require approval based on the "
            "current risk rules."
        )

    # --------------------------------------------------------
    # Prevent accidental submission of invalid AI result
    # --------------------------------------------------------

    submit_disabled = (
        validation_error is not None
        or analysis.start_date is None
        or analysis.end_date is None
    )

    submit_clicked = st.button(
        "🚀 Submit Backfill Request",
        type="primary",
        use_container_width=True,
        disabled=submit_disabled
    )

    if submit_clicked:

        try:

            with st.spinner(
                "Validating and submitting backfill request..."
            ):

                request = service.submit_request(
                    request_text=(
                        st.session_state.analysis_request_text
                    ),
                    requested_by=requested_by.strip()
                )

            st.session_state.submitted_request_id = (
                request.request_id
            )

            st.success(
                "Backfill request submitted successfully."
            )

            st.info(
                f"Request ID: **{request.request_id}**"
            )

            if request.approval_required:

                st.warning(
                    "The request is now **PENDING** approval. "
                    "Execution will not start until the request is approved."
                )

            else:

                st.info(
                    "The request is now **PENDING** and can proceed "
                    "according to the configured execution workflow."
                )

        except Exception as exc:

            st.error(
                "Backfill request submission failed."
            )

            st.exception(exc)


# ============================================================
# Submitted Request Confirmation
# ============================================================

if st.session_state.submitted_request_id:

    st.divider()

    st.success(
        f"""
        Backfill request created successfully.

        **Request ID:** `{st.session_state.submitted_request_id}`

        Navigate to **Requests & Approvals** to review the request.
        """
    )
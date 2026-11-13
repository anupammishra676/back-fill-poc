import streamlit as st

from streamlit_app.utils.backfill_ui_service import (
    BackfillUIService
)


st.set_page_config(
    page_title="Execution Monitor",
    page_icon="🚀",
    layout="wide"
)


# ============================================================
# Header
# ============================================================

st.title("🚀 Backfill Execution Monitor")

st.caption(
    "Execute approved backfill requests and monitor "
    "Databricks workflow execution."
)


# ============================================================
# Service
# ============================================================

@st.cache_resource
def get_service():

    return BackfillUIService()


service = get_service()


# ============================================================
# Refresh
# ============================================================

if st.button(
    "🔄 Refresh",
    use_container_width=False
):

    st.rerun()


st.divider()


# ============================================================
# Load Requests
# ============================================================

try:

    requests = service.get_requests()

except Exception as exc:

    st.error(
        f"Unable to load backfill requests: {exc}"
    )

    st.stop()


# ============================================================
# Eligible Requests
# ============================================================

eligible_requests = [
    request
    for request in requests
    if request["status"] in {
        "APPROVED",
        "EXECUTION_REQUESTED",
        "RUNNING",
        "SUCCESS",
        "FAILED"
    }
]


if not eligible_requests:

    st.info(
        "No approved or executed backfill requests "
        "are available."
    )

    st.stop()


# ============================================================
# Request Selector
# ============================================================

request_options = {
    (
        f"{request['request_id']} | "
        f"{request['status']} | "
        f"{request['start_date']} → "
        f"{request['end_date']}"
    ): request["request_id"]

    for request in eligible_requests
}


selected_label = st.selectbox(
    "Select Backfill Request",
    options=list(request_options.keys())
)


selected_request_id = request_options[
    selected_label
]


request = service.get_request(
    selected_request_id
)


if request is None:

    st.error(
        "Selected request could not be found."
    )

    st.stop()


# ============================================================
# Request Overview
# ============================================================

st.subheader("Backfill Request")


col1, col2, col3, col4 = st.columns(4)


with col1:

    st.metric(
        "Request ID",
        request["request_id"]
    )


with col2:

    st.metric(
        "Business Unit",
        request["business_unit"]
    )


with col3:

    st.metric(
        "Risk Level",
        request["risk_level"]
    )


with col4:

    st.metric(
        "Status",
        request["status"]
    )


# ============================================================
# Risk Explanation
# ============================================================

st.subheader("🧠 AI Risk Assessment")

risk_level = request["risk_level"]

if risk_level == "LOW":

    st.success(
        f"Risk Level: {risk_level}"
    )

elif risk_level == "MEDIUM":

    st.warning(
        f"Risk Level: {risk_level}"
    )

elif risk_level == "HIGH":

    st.error(
        f"Risk Level: {risk_level}"
    )

else:

    st.info(
        f"Risk Level: {risk_level}"
    )


risk_reason = request.get(
    "risk_reason"
)


if risk_reason:

    st.write(
        "**Why was this request classified as "
        f"{risk_level} risk?**"
    )

    st.info(
        risk_reason
    )

else:

    st.warning(
        "Risk explanation is not available for this request."
    )


# ============================================================
# Request Details
# ============================================================

with st.expander(
    "📋 View Request Details",
    expanded=False
):

    col1, col2 = st.columns(2)

    with col1:

        st.write("**Start Date**")
        st.write(request["start_date"])

        st.write("**End Date**")
        st.write(request["end_date"])

        st.write("**Requested By**")
        st.write(request["requested_by"])

    with col2:

        st.write("**Approval Required**")
        st.write(
            request["approval_required"]
        )

        st.write("**Approved By**")
        st.write(
            request["approved_by"]
            or "N/A"
        )

        st.write("**Approved At**")
        st.write(
            request["approved_at"]
            or "N/A"
        )


    st.write("**Original Request**")

    st.info(
        request["request_text"]
    )


# ============================================================
# Impact Analysis
# ============================================================

st.subheader("📊 Impact Analysis")


col1, col2 = st.columns(2)


with col1:

    st.write("**Business & Data Impact**")

    st.write(
        request["impact_summary"]
    )


with col2:

    st.write("**Proposed Processing Strategy**")

    st.write(
        request["proposed_strategy"]
    )


# ============================================================
# Execution
# ============================================================

st.divider()

st.subheader("⚙️ Execution")


# ============================================================
# APPROVED
# ============================================================

if request["status"] == "APPROVED":

    st.warning(
        "This request has been approved and is ready "
        "for execution."
    )

    if st.button(
        "▶️ Execute Backfill",
        type="primary",
        use_container_width=True
    ):

        try:

            with st.spinner(
                "Triggering Databricks backfill workflow..."
            ):

                result = service.execute_backfill(
                    request_id=selected_request_id
                )

            st.success(
                "Databricks backfill workflow triggered successfully."
            )

            st.write(
                f"**Execution ID:** "
                f"`{result['run_id']}`"
            )

            st.rerun()

        except Exception as exc:

            st.error(
                f"Backfill execution failed: {exc}"
            )


# ============================================================
# EXECUTION IN PROGRESS / COMPLETED
# ============================================================

elif request["status"] in {
    "EXECUTION_REQUESTED",
    "RUNNING",
    "SUCCESS",
    "FAILED"
}:

    try:

        execution = service.get_execution_status(
            request_id=selected_request_id
        )

    except Exception as exc:

        st.error(
            f"Unable to retrieve execution status: {exc}"
        )

        st.stop()


    status = execution["status"]


    # --------------------------------------------------------
    # Status Banner
    # --------------------------------------------------------

    if status == "RUNNING":

        st.info(
            "🔄 Backfill workflow is currently running."
        )

    elif status == "SUCCESS":

        st.success(
            "✅ Backfill workflow completed successfully."
        )

    elif status == "FAILED":

        st.error(
            "❌ Backfill workflow failed."
        )


    # --------------------------------------------------------
    # Execution Summary
    # --------------------------------------------------------

    col1, col2, col3 = st.columns(3)


    with col1:

        st.metric(
            "Execution Status",
            status
        )


    with col2:

        st.metric(
            "Databricks Run ID",
            execution["execution_id"]
            or "N/A"
        )


    with col3:

        st.metric(
            "Job",
            service.JOB_NAME
        )


    # --------------------------------------------------------
    # Workflow State
    # --------------------------------------------------------

    st.subheader(
        "Databricks Workflow State"
    )


    col1, col2, col3 = st.columns(3)


    with col1:

        st.write("**Lifecycle State**")

        st.code(
            execution["life_cycle_state"]
            or "N/A"
        )


    with col2:

        st.write("**Result State**")

        st.code(
            execution["result_state"]
            or "N/A"
        )


    with col3:

        st.write("**State Message**")

        st.code(
            execution["state_message"]
            or "N/A"
        )


    # --------------------------------------------------------
    # Timeline
    # --------------------------------------------------------

    st.subheader(
        "🕒 Execution Timeline"
    )


    col1, col2 = st.columns(2)


    with col1:

        st.write(
            "**Execution Started**"
        )

        st.write(
            execution["execution_started_at"]
            or "N/A"
        )


    with col2:

        st.write(
            "**Execution Completed**"
        )

        st.write(
            execution["execution_completed_at"]
            or "N/A"
        )


    # --------------------------------------------------------
    # Error
    # --------------------------------------------------------

    if (
        status == "FAILED"
        and execution.get("execution_error")
    ):

        st.subheader(
            "❌ Error Details"
        )

        st.error(
            execution["execution_error"]
        )


    # --------------------------------------------------------
    # Monitoring
    # --------------------------------------------------------

    if status == "RUNNING":

        st.divider()

        st.info(
            "The Databricks workflow is still running. "
            "Click **Refresh** to retrieve the latest status."
        )


# ============================================================
# Fallback
# ============================================================

else:

    st.info(
        f"Current request status: {request['status']}"
    )
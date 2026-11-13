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
    page_title="Requests & Approvals",
    page_icon="✅",
    layout="wide"
)


# ============================================================
# Page Header
# ============================================================

st.title("Requests & Approvals")

st.markdown(
    """
    Review submitted backfill requests and perform the required
    approval or rejection action.
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
# Helper: Load Requests
# ============================================================

def load_requests():

    query = """
        SELECT
            request_id,
            requested_by,
            request_text,
            business_unit,
            start_date,
            end_date,
            status,
            risk_level,
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
        FROM data_engineering.backfill_poc.backfill_request
        ORDER BY created_at DESC
    """

    with service._get_connection() as connection:

        with connection.cursor() as cursor:

            cursor.execute(query)

            columns = [
                column[0]
                for column in cursor.description
            ]

            rows = cursor.fetchall()

    return columns, rows


# ============================================================
# Refresh
# ============================================================

refresh_col, _ = st.columns([1, 5])

with refresh_col:

    if st.button(
        "🔄 Refresh",
        use_container_width=True
    ):
        st.rerun()


# ============================================================
# Load Requests
# ============================================================

try:

    with st.spinner("Loading backfill requests..."):

        columns, rows = load_requests()

except Exception as exc:

    st.error(
        "Unable to load backfill requests."
    )

    st.exception(exc)

    st.stop()


# ============================================================
# No Requests
# ============================================================

if not rows:

    st.info(
        "No backfill requests found."
    )

    st.stop()


# ============================================================
# Convert Rows to Dictionaries
# ============================================================

requests = [
    dict(zip(columns, row))
    for row in rows
]


# ============================================================
# Request Summary
# ============================================================

total_requests = len(requests)

pending_requests = sum(
    1
    for request in requests
    if request["status"] == "PENDING"
)

approved_requests = sum(
    1
    for request in requests
    if request["status"] == "APPROVED"
)

rejected_requests = sum(
    1
    for request in requests
    if request["status"] == "REJECTED"
)


metric1, metric2, metric3, metric4 = st.columns(4)

with metric1:
    st.metric(
        "Total Requests",
        total_requests
    )

with metric2:
    st.metric(
        "Pending",
        pending_requests
    )

with metric3:
    st.metric(
        "Approved",
        approved_requests
    )

with metric4:
    st.metric(
        "Rejected",
        rejected_requests
    )


st.divider()


# ============================================================
# Status Filter
# ============================================================

st.subheader("Backfill Requests")

filter_col1, filter_col2 = st.columns([1, 3])

with filter_col1:

    status_filter = st.selectbox(
        "Status",
        [
            "ALL",
            "PENDING",
            "APPROVED",
            "REJECTED"
        ]
    )


filtered_requests = requests

if status_filter != "ALL":

    filtered_requests = [
        request
        for request in requests
        if request["status"] == status_filter
    ]


# ============================================================
# Request Table
# ============================================================

if not filtered_requests:

    st.info(
        f"No {status_filter.lower()} requests found."
    )

    st.stop()


table_data = []

for request in filtered_requests:

    table_data.append(
        {
            "Request ID": request["request_id"],
            "Business Unit": request["business_unit"],
            "Start Date": str(request["start_date"]),
            "End Date": str(request["end_date"]),
            "Risk": request["risk_level"],
            "Approval": (
                "Required"
                if request["approval_required"]
                else "Not Required"
            ),
            "Status": request["status"],
            "Requested By": request["requested_by"],
            "Created": str(request["created_at"])
        }
    )


st.dataframe(
    table_data,
    use_container_width=True,
    hide_index=True
)


# ============================================================
# Request Selection
# ============================================================

st.divider()

st.subheader("Review Request")


request_options = [
    request["request_id"]
    for request in filtered_requests
]


selected_request_id = st.selectbox(
    "Select Request",
    request_options
)


selected_request = next(
    request
    for request in filtered_requests
    if request["request_id"] == selected_request_id
)


# ============================================================
# Request Details
# ============================================================

detail_col1, detail_col2 = st.columns(2)

with detail_col1:

    st.markdown("#### Request Information")

    st.write(
        f"**Request ID:** `{selected_request['request_id']}`"
    )

    st.write(
        f"**Requested By:** {selected_request['requested_by']}"
    )

    st.write(
        f"**Business Unit:** {selected_request['business_unit']}"
    )

    st.write(
        f"**Risk Level:** {selected_request['risk_level']}"
    )

    st.write(
        f"**Status:** {selected_request['status']}"
    )

    st.write(
        f"**Approval Required:** "
        f"{selected_request['approval_required']}"
    )


with detail_col2:

    st.markdown("#### Backfill Window")

    st.write(
        f"**Start Date:** {selected_request['start_date']}"
    )

    st.write(
        f"**End Date:** {selected_request['end_date']}"
    )

    st.write(
        f"**Created At:** {selected_request['created_at']}"
    )

    st.write(
        f"**Updated At:** {selected_request['updated_at']}"
    )


# ============================================================
# Original Request
# ============================================================

st.markdown("#### Original Request")

st.info(
    selected_request["request_text"]
)


# ============================================================
# AI Impact Analysis
# ============================================================

st.markdown("#### AI Impact Analysis")

st.write(
    selected_request["impact_summary"]
)


# ============================================================
# Proposed Strategy
# ============================================================

st.markdown("#### Proposed Strategy")

st.success(
    selected_request["proposed_strategy"]
)


# ============================================================
# Approval / Rejection
# ============================================================

if (
    selected_request["status"] == "PENDING"
    and selected_request["approval_required"]
):

    st.divider()

    st.subheader("Approval Decision")

    approver = st.text_input(
        "Approver",
        placeholder="Enter approver name"
    )

    action_col1, action_col2 = st.columns(2)

    # --------------------------------------------------------
    # Approve
    # --------------------------------------------------------

    with action_col1:

        approve_clicked = st.button(
            "✅ Approve Request",
            type="primary",
            use_container_width=True
        )

        if approve_clicked:

            if not approver.strip():

                st.warning(
                    "Please enter the approver name."
                )

            else:

                try:

                    with st.spinner(
                        "Approving request..."
                    ):

                        service.approve_request(
                            request_id=selected_request_id,
                            approved_by=approver.strip()
                        )

                    st.success(
                        f"Request `{selected_request_id}` "
                        "has been approved."
                    )

                    st.rerun()

                except Exception as exc:

                    st.error(
                        "Unable to approve the request."
                    )

                    st.exception(exc)

    # --------------------------------------------------------
    # Reject
    # --------------------------------------------------------

    with action_col2:

        reject_clicked = st.button(
            "❌ Reject Request",
            use_container_width=True
        )

        rejection_reason = st.text_input(
            "Rejection Reason",
            placeholder="Reason for rejecting the request"
        )

        if reject_clicked:

            if not approver.strip():

                st.warning(
                    "Please enter the approver/reviewer name."
                )

            elif not rejection_reason.strip():

                st.warning(
                    "Please provide a rejection reason."
                )

            else:

                try:

                    with st.spinner(
                        "Rejecting request..."
                    ):

                        service.reject_request(
                            request_id=selected_request_id,
                            rejected_by=approver.strip(),
                            rejection_reason=rejection_reason.strip()
                        )

                    st.success(
                        f"Request `{selected_request_id}` "
                        "has been rejected."
                    )

                    st.rerun()

                except Exception as exc:

                    st.error(
                        "Unable to reject the request."
                    )

                    st.exception(exc)


elif selected_request["status"] == "PENDING":

    st.info(
        "This request does not require approval based on the "
        "current risk rules."
    )

elif selected_request["status"] == "APPROVED":

    st.success(
        f"This request was approved by "
        f"{selected_request['approved_by']} "
        f"at {selected_request['approved_at']}."
    )

elif selected_request["status"] == "REJECTED":

    st.error(
        f"This request was rejected by "
        f"{selected_request['rejected_by']}."
    )

    if selected_request["rejection_reason"]:

        st.write(
            f"**Reason:** {selected_request['rejection_reason']}"
        )
import streamlit as st



st.set_page_config(
    page_title="Natural language based AI Orchestrator",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)


# ============================================================
# CUSTOM CSS
# ============================================================

st.markdown(
    """
    <style>

    .main-title {
        font-size: 2.4rem;
        font-weight: 700;
        margin-bottom: 0.2rem;
    }

    .subtitle {
        font-size: 1.05rem;
        color: #666;
        margin-bottom: 2rem;
    }

    .metric-card {
        padding: 1rem;
        border-radius: 0.75rem;
        border: 1px solid #ddd;
        background-color: #fafafa;
    }

    </style>
    """,
    unsafe_allow_html=True
)


# ============================================================
# SIDEBAR
# ============================================================

with st.sidebar:

    st.markdown("## 🔄 Backfill AI")

    st.markdown(
        """
        **Natural language based AI Orchestrator**

        Intelligent backfill request analysis,
        validation, approval and execution.
        """
    )

    st.divider()

    st.markdown("### Navigation")

    st.page_link(
        "pages/01_Backfill_Request.py",
        label="🧠 Backfill Request"
    )

    st.page_link(
        "pages/02_Requests_Approvals.py",
        label="📋 Requests & Approvals"
    )

    st.page_link(
        "pages/03_Execution_Monitor.py",
        label="📊 Execution Monitor"
    )

    st.divider()

    st.caption(
        "Backfill POC"
    )


# ============================================================
# LANDING PAGE
# ============================================================

st.markdown(
    '<div class="main-title">Natural language based AI Orchestrator</div>',
    unsafe_allow_html=True
)

st.markdown(
    """
    <div class="subtitle">
    Convert natural-language backfill requests into
    validated, governed and observable pipeline executions.
    </div>
    """,
    unsafe_allow_html=True
)


# ============================================================
# HIGH LEVEL FLOW
# ============================================================

col1, col2, col3, col4 = st.columns(4)

with col1:

    st.markdown("### 🧠")

    st.markdown("**1. Describe**")

    st.caption(
        "Submit a backfill request using natural language."
    )


with col2:

    st.markdown("### 🔍")

    st.markdown("**2. Analyze**")

    st.caption(
        "AI extracts dates, business unit, risk and strategy."
    )


with col3:

    st.markdown("### 🛡️")

    st.markdown("**3. Govern**")

    st.caption(
        "Validate impact and determine whether approval is required."
    )


with col4:

    st.markdown("### 🚀")

    st.markdown("**4. Execute**")

    st.caption(
        "Trigger the Databricks pipeline and monitor execution."
    )


st.divider()


st.info(
    "Use the **Backfill Request** page to submit your first request."
)
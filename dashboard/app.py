from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

# ==================================================
# PAGE CONFIGURATION
# ==================================================
st.set_page_config(
    page_title="SellPulse | Product Rankings",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

st_autorefresh(interval=15 * 1000, key="dashboard_refresh")

ROOT = Path(__file__).resolve().parents[1]
RESULT_FILE = ROOT / "results" / "top10_products.csv"

BLUE = "#4169E1"
PURPLE = "#8067ED"
NAVY = "#172642"
MUTED = "#718096"
BORDER = "#E3EAF4"


# ==================================================
# STYLING: COLORS, NAVIGATION, HOVER EFFECTS
# ==================================================
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

.stApp {
    background: #F5F7FC;
    font-family: 'Inter', sans-serif;
    color: #172642;
}

.block-container {
    max-width: 1500px;
    padding: 1.7rem 2rem 2.5rem;
}

h1, h2, h3 {
    color: #172642 !important;
    font-weight: 750 !important;
}

[data-testid="stHeader"] {
    background: rgba(245,247,252,0.96);
}

/* Sidebar */
[data-testid="stSidebar"] {
    background: #14213D;
    border-right: 1px solid #263958;
}

[data-testid="stSidebar"] * {
    color: #E8EEFA;
}

[data-testid="stSidebar"] .brand {
    color: #FFFFFF;
    font-size: 28px;
    font-weight: 800;
    letter-spacing: -1px;
}

[data-testid="stSidebar"] .brand-sub {
    color: #A6B8D9;
    font-size: 10px;
    letter-spacing: 2px;
    margin-top: 4px;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label {
    border: 1px solid transparent;
    border-radius: 9px;
    padding: 9px 10px;
    transition: background .2s ease,
                border-color .2s ease,
                transform .2s ease;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label:hover {
    background: #263958;
    border-color: #40557A;
    transform: translateX(3px);
}

[data-testid="stSidebar"] [data-testid="stRadio"] label:has(input:checked) {
    background: linear-gradient(100deg, #355BD5, #6754C9);
    border-color: #879BFF;
}

[data-testid="stSidebar"] [data-testid="stRadio"] label p {
    color: #FFFFFF !important;
    font-weight: 500;
}

/* KPI cards */
[data-testid="stMetric"] {
    background: #FFFFFF;
    border: 1px solid #E3EAF4;
    border-radius: 13px;
    padding: 17px 19px;
    transition: transform .2s ease,
                box-shadow .2s ease,
                border-color .2s ease;
}

[data-testid="stMetric"]:hover {
    transform: translateY(-3px);
    border-color: #B8C8FF;
    box-shadow: 0 8px 22px rgba(49,84,198,.10);
}

[data-testid="stMetricLabel"] {
    color: #718096 !important;
}

[data-testid="stMetricValue"] {
    color: #172642 !important;
    font-weight: 750 !important;
    font-size: 25px !important;
}

/* Content containers */
[data-testid="stVerticalBlockBorderWrapper"] {
    background: #FFFFFF;
    border: 1px solid #E3EAF4;
    border-radius: 13px;
    transition: border-color .2s ease,
                box-shadow .2s ease;
}

[data-testid="stVerticalBlockBorderWrapper"]:hover {
    border-color: #CAD5ED;
    box-shadow: 0 4px 15px rgba(23,38,66,.045);
}

[data-testid="stDataFrame"] {
    border: 1px solid #E3EAF4;
    border-radius: 10px;
    overflow: hidden;
}

/* Buttons */
.stButton > button,
.stDownloadButton > button {
    background: #4169E1;
    color: #FFFFFF !important;
    border: 1px solid #4169E1;
    border-radius: 8px;
    font-weight: 600;
    transition: background .2s ease,
                box-shadow .2s ease,
                transform .2s ease;
}

.stButton > button:hover,
.stDownloadButton > button:hover {
    background: #3154C6 !important;
    color: #FFFFFF !important;
    border-color: #3154C6 !important;
    box-shadow: 0 4px 12px rgba(49,84,198,.20);
    transform: translateY(-1px);
}

/* Inputs and selectors */
div[data-baseweb="input"] > div,
div[data-baseweb="select"] > div {
    background: #FFFFFF;
    border-radius: 8px;
    border-color: #D9E2F0;
    transition: border-color .2s ease,
                box-shadow .2s ease;
}

div[data-baseweb="input"] > div:hover,
div[data-baseweb="select"] > div:hover {
    border-color: #91A8EE;
}

div[data-baseweb="input"] > div:focus-within,
div[data-baseweb="select"] > div:focus-within {
    border-color: #4169E1 !important;
    box-shadow: 0 0 0 3px rgba(65,105,225,.12);
}

.section-title {
    color: #172642;
    font-size: 17px;
    font-weight: 700;
}

.subtext {
    color: #718096;
    font-size: 12px;
}

hr {
    border-color: #E3EAF4;
}

footer {
    visibility: hidden;
}

@media (prefers-reduced-motion: reduce) {
    *, *::before, *::after {
        transition: none !important;
        animation: none !important;
    }
}
</style>
""", unsafe_allow_html=True)


# ==================================================
# LOAD AND VALIDATE RANKING DATA
# ==================================================
@st.cache_data
def load_data(path, modified_time):
    data = pd.read_csv(path)
    data.columns = data.columns.str.strip()

    required = {"product_name", "units_sold"}
    if not required.issubset(data.columns):
        raise ValueError(
            "CSV must contain 'product_name' and 'units_sold' columns."
        )

    data["product_name"] = data["product_name"].fillna("").astype(str).str.strip()
    data["units_sold"] = pd.to_numeric(data["units_sold"], errors="coerce")

    data = data.dropna(subset=["units_sold"])
    data = data[(data["units_sold"] >= 0) & (data["product_name"] != "")]

    # Combine repeated product names before calculating the top 10.
    data = (
        data.groupby("product_name", as_index=False)["units_sold"]
        .sum()
        .sort_values(
            ["units_sold", "product_name"],
            ascending=[False, True],
        )
        .head(10)
        .reset_index(drop=True)
    )

    data["units_sold"] = data["units_sold"].round().astype(int)
    data.insert(0, "rank", range(1, len(data) + 1))
    return data


# ==================================================
# SIDEBAR NAVIGATION
# ==================================================
with st.sidebar:
    st.markdown(
        '<div class="brand">◈ SellPulse</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="brand-sub">PRODUCT RANKING ENGINE</div>',
        unsafe_allow_html=True,
    )
    st.divider()
    st.caption("DASHBOARD")

    page = st.radio(
        "Navigation",
        ["Overview", "Product Rankings", "Sales Analysis"],
        format_func=lambda p: {
            "Overview": "⌂   Overview",
            "Product Rankings": "🏆   Product Rankings",
            "Sales Analysis": "▥   Sales Analysis",
        }[p],
        label_visibility="collapsed",
        key="navigation",
    )


# ==================================================
# CHECK RESULT FILE AND LOAD DATA
# ==================================================
if not RESULT_FILE.exists():
    st.error(
        f"Ranking results were not found at:\n\n`{RESULT_FILE}`\n\n"
        "Create `results/top10_products.csv` before starting the dashboard."
    )
    st.stop()

try:
    modified_time = RESULT_FILE.stat().st_mtime
    df = load_data(str(RESULT_FILE), modified_time)
except (OSError, ValueError, pd.errors.ParserError) as error:
    st.error(f"Could not load ranking data: {error}")
    st.stop()

if df.empty:
    st.warning("No valid product sales data is available.")
    st.stop()

total_units = int(df["units_sold"].sum())
top_product = df.iloc[0]["product_name"]
top_units = int(df.iloc[0]["units_sold"])


# ==================================================
# SHARED HEADER AND REFRESH
# ==================================================
header, action = st.columns([4, 1])

with header:
    st.markdown(
        f'<div class="subtext">SELLPULSE / {page.upper()}</div>',
        unsafe_allow_html=True,
    )

with action:
    if st.button("↻ Refresh", use_container_width=True):
        st.cache_data.clear()
        st.rerun()


# ==================================================
# REUSABLE, CLEAR RANKING BAR CHART
# ==================================================
def show_ranking_bar_chart(data, height=530):
    """Display a labeled horizontal bar chart with readable axes and tooltips."""
    chart_df = data.sort_values("units_sold", ascending=True).copy()

    fig = px.bar(
        chart_df,
        x="units_sold",
        y="product_name",
        orientation="h",
        text="units_sold",
        custom_data=["rank"],
        color="units_sold",
        color_continuous_scale=[
            "#DCE7FF",
            "#7799F7",
            "#4169E1",
            "#6246C7",
        ],
        labels={
            "units_sold": "Units Sold",
            "product_name": "Product Name",
        },
    )

    fig.update_traces(
        texttemplate="%{x:,.0f} units",
        textposition="outside",
        textfont=dict(family="Arial", size=12, color=NAVY),
        cliponaxis=False,
        marker_line_width=0,
        hovertemplate=(
            "<b>%{y}</b><br>"
            "Rank: #%{customdata[0]}<br>"
            "Total units sold: %{x:,.0f}"
            "<extra></extra>"
        ),
    )

    max_units = int(chart_df["units_sold"].max()) if not chart_df.empty else 0
    # Leave room to display exact bar-end labels.
    x_max = max_units * 1.25 if max_units > 0 else 1

    fig.update_layout(
        template="plotly_white",
        height=height,
        autosize=True,
        showlegend=False,
        coloraxis_showscale=False,
        bargap=0.32,
        margin=dict(l=15, r=100, t=25, b=75),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="#FFFFFF",
        font=dict(family="Arial", size=12, color=NAVY),
        hoverlabel=dict(
            bgcolor=NAVY,
            bordercolor=BLUE,
            font=dict(family="Arial", size=13, color="#FFFFFF"),
        ),
        xaxis=dict(
            title=dict(
                text="Number of Units Sold",
                font=dict(size=13, color=NAVY),
                standoff=18,
            ),
            tickformat=",d",
            range=[0, x_max],
            showgrid=True,
            gridcolor="#E7EDF6",
            gridwidth=1,
            zeroline=False,
            ticks="outside",
            tickfont=dict(size=11, color="#52627A"),
        ),
        yaxis=dict(
            title=dict(
                text="Product Name",
                font=dict(size=13, color=NAVY),
                standoff=12,
            ),
            showgrid=False,
            ticks="outside",
            automargin=True,
            tickfont=dict(size=12, color="#263956"),
        ),
        uniformtext=dict(minsize=10, mode="show"),
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
            "responsive": True,
            "modeBarButtonsToRemove": ["lasso2d", "select2d"],
        },
    )


# ==================================================
# 1. OVERVIEW
# ==================================================
if page == "Overview":
    st.title("Product Sales Overview")
    st.markdown(
        '<p class="subtext">A clear view of your top-selling products.</p>',
        unsafe_allow_html=True,
    )

    metric1, metric2, metric3 = st.columns(3)
    metric1.metric("Products Ranked", len(df))
    metric2.metric("Units Sold · Top 10", f"{total_units:,}")
    metric3.metric("Best Seller", top_product, f"{top_units:,} units")

    st.write("")

    with st.container(border=True):
        st.markdown(
            '<div class="section-title">Top 10 Products by Units Sold</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<p class="subtext">Ranked from highest to lowest sales volume. '
            'Bar-end labels show exact units sold. Hover over a bar for its rank.</p>',
            unsafe_allow_html=True,
        )
        show_ranking_bar_chart(df, height=530)

    with st.container(border=True):
        st.markdown(
            '<div class="section-title">Product Ranking Table</div>',
            unsafe_allow_html=True,
        )
        st.dataframe(
            df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "rank": st.column_config.NumberColumn("Rank"),
                "product_name": st.column_config.TextColumn("Product Name"),
                "units_sold": st.column_config.NumberColumn(
                    "Units Sold", format="%d"
                ),
            },
        )


# ==================================================
# 2. PRODUCT RANKINGS
# ==================================================
elif page == "Product Rankings":
    st.title("Product Rankings")
    st.markdown(
        '<p class="subtext">Search, sort, and export the ranked products.</p>',
        unsafe_allow_html=True,
    )

    search_col, sort_col = st.columns([2, 1])

    with search_col:
        search = st.text_input(
            "Search product",
            placeholder="Enter a product name...",
        )

    with sort_col:
        order = st.selectbox(
            "Sort by",
            ["Highest units sold", "Lowest units sold", "Product name"],
        )

    result = df[
        df["product_name"].str.contains(search.strip(), case=False, na=False)
    ].copy()

    if order == "Lowest units sold":
        result = result.sort_values("units_sold", ascending=True)
    elif order == "Product name":
        result = result.sort_values("product_name", ascending=True)
    else:
        result = result.sort_values("units_sold", ascending=False)

    # Recalculate visible ranks for the selected ordering.
    result = result.reset_index(drop=True)
    result["rank"] = range(1, len(result) + 1)

    with st.container(border=True):
        st.markdown(
            f'<div class="section-title">Ranking Results · {len(result)} products</div>',
            unsafe_allow_html=True,
        )
        st.dataframe(
            result,
            use_container_width=True,
            hide_index=True,
            column_config={
                "rank": st.column_config.NumberColumn("Rank"),
                "product_name": st.column_config.TextColumn("Product Name"),
                "units_sold": st.column_config.NumberColumn(
                    "Units Sold", format="%d"
                ),
            },
        )

        st.download_button(
            "Download CSV",
            data=result.to_csv(index=False).encode("utf-8"),
            file_name="top10_products.csv",
            mime="text/csv",
        )


# ==================================================
# 3. SALES ANALYSIS
# ==================================================
elif page == "Sales Analysis":
    st.title("Sales Analysis")
    st.markdown(
        '<p class="subtext">Compare product volumes and understand their '
        'contribution to top-10 sales.</p>',
        unsafe_allow_html=True,
    )

    analysis = st.selectbox(
        "Choose analysis",
        [
            "Product comparison",
            "Sales contribution",
            "Cumulative units sold",
        ],
    )

    if analysis == "Product comparison":
        with st.container(border=True):
            st.markdown(
                '<div class="section-title">Units Sold by Product</div>',
                unsafe_allow_html=True,
            )
            st.caption(
                "Exact unit labels are shown at the end of each bar."
            )
            show_ranking_bar_chart(df, height=530)

    elif analysis == "Sales contribution":
        with st.container(border=True):
            st.markdown(
                '<div class="section-title">Share of Top-10 Units Sold</div>',
                unsafe_allow_html=True,
            )

            pie = px.pie(
                df,
                names="product_name",
                values="units_sold",
                hole=0.52,
                color_discrete_sequence=px.colors.qualitative.Set2,
            )

            pie.update_traces(
                textposition="inside",
                textinfo="percent",
                hovertemplate=(
                    "<b>%{label}</b><br>"
                    "Units sold: %{value:,.0f}<br>"
                    "Share: %{percent}"
                    "<extra></extra>"
                ),
                marker=dict(line=dict(color="#FFFFFF", width=2)),
            )

            pie.update_layout(
                template="plotly_white",
                height=480,
                margin=dict(l=15, r=15, t=25, b=25),
                paper_bgcolor="rgba(0,0,0,0)",
                hoverlabel=dict(
                    bgcolor=NAVY,
                    bordercolor=BLUE,
                    font=dict(color="#FFFFFF", size=13),
                ),
                legend=dict(
                    orientation="h",
                    yanchor="bottom",
                    y=-0.15,
                ),
            )

            st.plotly_chart(pie, use_container_width=True)
            st.caption(
                "Percentages represent each product's share of the displayed "
                "top-10 units, not all company sales."
            )

    else:
        with st.container(border=True):
            st.markdown(
                '<div class="section-title">Cumulative Units Sold by Rank</div>',
                unsafe_allow_html=True,
            )

            cumulative = df.sort_values("rank").copy()
            cumulative["cumulative_units"] = cumulative["units_sold"].cumsum()

            fig = px.line(
                cumulative,
                x="rank",
                y="cumulative_units",
                markers=True,
                labels={
                    "rank": "Product Rank",
                    "cumulative_units": "Cumulative Units Sold",
                },
            )

            fig.update_traces(
                line=dict(color=BLUE, width=3),
                marker=dict(color=PURPLE, size=9),
                hovertemplate=(
                    "Rank: %{x}<br>"
                    "Cumulative units: %{y:,.0f}"
                    "<extra></extra>"
                ),
            )

            fig.update_layout(
                template="plotly_white",
                height=430,
                margin=dict(l=15, r=25, t=25, b=55),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="#FFFFFF",
                font=dict(family="Arial", color=NAVY),
                hoverlabel=dict(
                    bgcolor=NAVY,
                    bordercolor=BLUE,
                    font=dict(color="#FFFFFF"),
                ),
                xaxis=dict(
                    title="Product Rank (1 = highest sales)",
                    dtick=1,
                    gridcolor="#EAF0F7",
                ),
                yaxis=dict(
                    title="Cumulative Units Sold",
                    tickformat=",d",
                    gridcolor="#EAF0F7",
                ),
            )

            st.plotly_chart(fig, use_container_width=True)
            st.caption(
                "This chart accumulates units across the ranked products; "
                "it does not represent changes in sales over time."
            )


# ==================================================
# FOOTER
# ==================================================
st.divider()
st.markdown(
    '<div style="text-align:center;color:#8794A8;font-size:11px">'
    'SellPulse · Top-Selling Product Ranking Engine'
    '</div>',
    unsafe_allow_html=True,
)

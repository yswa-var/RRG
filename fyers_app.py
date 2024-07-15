import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta

# Assume these are imported from your existing codebase
from db.db_ops import DatabaseManager
from indexes import EquiWeightIndex

st.set_page_config(page_title="Dynamic RRG Graphs", layout="wide")


def load_data(db):
    idx = EquiWeightIndex(db)
    index_data = idx.calculate_industry_indices()
    return index_data


def load_tickers():
    return pd.read_csv('tickers/nifty200.csv')


def calculate_rrg_components_improved(data, benchmark, length=14):
    rs = data.div(benchmark, axis=0)

    wma_rs = rs.rolling(window=length, center=False).apply(
        lambda x: np.sum(x * np.arange(1, length + 1)) / np.sum(np.arange(1, length + 1)))

    rs_ratio = (rs / wma_rs).rolling(window=length, center=False).apply(
        lambda x: np.sum(x * np.arange(1, length + 1)) / np.sum(np.arange(1, length + 1))) * 100

    rs_momentum = rs_ratio / rs_ratio.rolling(window=length, center=False).apply(
        lambda x: np.sum(x * np.arange(1, length + 1)) / np.sum(np.arange(1, length + 1))) * 100
    return rs_ratio, rs_momentum


def normalize_data(data):
    return (data - data.mean()) / data.std()


def create_rrg_plot(normalized_rs_ratio, normalized_rs_momentum, ticker, trail_length):
    fig = go.Figure()

    # Plot trail
    fig.add_trace(go.Scatter(
        x=normalized_rs_ratio.iloc[-trail_length:],
        y=normalized_rs_momentum.iloc[-trail_length:],
        mode='lines',
        line=dict(width=1, color='rgb(247, 251, 255)')
    ))

    # Plot current position
    fig.add_trace(go.Scatter(
        x=[normalized_rs_ratio.iloc[-1]],
        y=[normalized_rs_momentum.iloc[-1]],
        mode='markers+text',
        marker=dict(size=10),
        text=[ticker],
        textposition="top center",
        name=ticker
    ))

    fig.add_shape(type="line", x0=0, y0=-6, x1=0, y1=6, line=dict(color="white", width=1))
    fig.add_shape(type="line", x0=-6, y0=0, x1=6, y1=0, line=dict(color="white", width=1))

    fig.update_layout(
        title=f"RRG for {ticker}",
        xaxis_title="RS-Ratio (Relative Strength)",
        yaxis_title="RS-Momentum",
        showlegend=False,
        height=500,
        width=700
    )

    return fig


def process_data(trail_length, industries, benchmark, data):
    rs_ratio, rs_momentum = calculate_rrg_components_improved(data[industries], data[benchmark])

    normalized_rs_ratio = normalize_data(rs_ratio)
    normalized_rs_momentum = normalize_data(rs_momentum)

    cols = st.columns(2)
    for i, industry in enumerate(industries):
        fig = create_rrg_plot(normalized_rs_ratio[industry], normalized_rs_momentum[industry], industry, trail_length)
        cols[i % 2].plotly_chart(fig, use_container_width=True)


def main():
    st.title("Dynamic Relative Rotation Graphs (RRG)")

    db = DatabaseManager()
    if db.engine is None:
        db.connect_to_database()

    data = load_data(db)
    tickers = load_tickers()

    st.sidebar.header("Settings")

    industries = st.sidebar.multiselect(
        "Select industries",
        options=data.columns[1:],  # Exclude the 'date' column
        default=data.columns[1:6]  # Default to first 5 industries
    )

    benchmark = st.sidebar.selectbox(
        "Select benchmark index",
        options=data.columns[1:],  # Exclude the 'date' column
        index=0  # Default to first industry
    )

    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)

    start_date = st.sidebar.date_input("Start Date", value=start_date)
    end_date = st.sidebar.date_input("End Date", value=end_date)

    trail_length = st.sidebar.slider("Trail Length (weeks)", min_value=1, max_value=52, value=13)

    if start_date < end_date and industries and benchmark:
        # Filter data based on date range
        mask = (data['date'] >= start_date.strftime('%Y-%m-%d')) & (data['date'] <= end_date.strftime('%Y-%m-%d'))
        filtered_data = data.loc[mask]

        process_data(trail_length, industries, benchmark, filtered_data)

    else:
        if start_date >= end_date:
            st.error("Error: End date must be after start date.")
        if not industries:
            st.error("Error: Please select at least one industry.")
        if not benchmark:
            st.error("Error: Please select a benchmark index.")

    # Display tickers for selected industries
    if industries:
        st.subheader("Stocks in Selected Industries")
        for industry in industries:
            st.write(f"**{industry}**")
            industry_tickers = tickers[tickers['Industry'] == industry]['Symbol'].tolist()
            st.write(", ".join(industry_tickers))
            st.write("")


if __name__ == "__main__":
    main()

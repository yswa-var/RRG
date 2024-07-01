import streamlit as st
import yfinance as yf
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(page_title="Dynamic RRG Graphs", layout="wide")


@st.cache_data
def download_data(tickers, start_date, end_date):
    data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))['Adj Close']
    return data


def calculate_rrg_components(data, benchmark):
    rs_ratio = data.div(benchmark, axis=0) * 100

    r12 = np.log(1 + data.pct_change(periods=252))
    r1 = np.log(1 + data.pct_change(periods=21))
    rs_momentum = r12 - r1

    return rs_ratio, rs_momentum


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
        x=normalized_rs_ratio.iloc[-trail_length * 5:],
        y=normalized_rs_momentum.iloc[-trail_length * 5:],
        mode='lines',
        line=dict(width=1, color='rgb(247, 251, 255)')
    ))

    # Plot current position
    fig.add_trace(go.Scatter(
        x=[normalized_rs_ratio.iloc[-1]],
        y=[normalized_rs_momentum.iloc[-1]],
        mode='markers+text',
        marker=dict(size=10),
        textposition="top center",
        name=ticker
    ))

    fig.add_shape(type="line", x0=0, y0=-10, x1=0, y1=10, line=dict(color="black", width=1))
    fig.add_shape(type="line", x0=-10, y0=0, x1=10, y1=0, line=dict(color="black", width=1))

    fig.update_layout(
        title=f"RRG for {ticker}",
        xaxis_title="RS-Ratio (Relative Strength)",
        yaxis_title="RS-Momentum",
        showlegend=False
    )

    return fig


def main():
    st.title("Dynamic Relative Rotation Graphs (RRG)")

    st.sidebar.header("Settings")
    equity_input = st.sidebar.text_area("Enter equity tickers (one per line):",
                                        value="^NSEBANK\nNIFTY_FIN_SERVICE.NS\n^CNXENERGY\n^CNXFMCG\n^CNXAUTO")
    benchmark_index = st.sidebar.text_input("Enter benchmark index ticker:", value="^CNX100")

    equity = [ticker.strip() for ticker in equity_input.split('\n') if ticker.strip()]

    end_date = datetime.now()
    start_date = end_date - timedelta(days=365 * 2)

    start_date = st.sidebar.date_input("Start Date", value=start_date)
    end_date = st.sidebar.date_input("End Date", value=end_date)

    trail_length = st.sidebar.slider("Trail Length (weeks)", min_value=1, max_value=52, value=13)

    if start_date < end_date and equity and benchmark_index:
        tickers = equity + [benchmark_index]
        data = download_data(tickers, start_date, end_date)

        rs_ratio, rs_momentum = calculate_rrg_components_improved(data[equity], data[benchmark_index])

        normalized_rs_ratio = normalize_data(rs_ratio)
        normalized_rs_momentum = normalize_data(rs_momentum)

        cols = st.columns(2)
        for i, ticker in enumerate(equity):
            fig = create_rrg_plot(normalized_rs_ratio[ticker], normalized_rs_momentum[ticker], ticker, trail_length)
            cols[i % 2].plotly_chart(fig, use_container_width=True)

    else:
        if start_date >= end_date:
            st.error("Error: End date must be after start date.")
        if not equity:
            st.error("Error: Please enter at least one equity ticker.")
        if not benchmark_index:
            st.error("Error: Please enter a benchmark index ticker.")


if __name__ == "__main__":
    main()
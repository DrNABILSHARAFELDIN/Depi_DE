import streamlit as st
import pandas as pd
import glob
import os
import numpy as np
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

st.set_page_config(page_title="Live Market Pipeline", layout="wide", page_icon="📈")

# Re-render UI frame every 2 seconds to capture Spark outputs
st_autorefresh(interval=10000, key="pipeline-refresh")

st.title("⚡ Real-Time Global Crypto Trade Analytics")
st.markdown("Processing live streaming market data from **Binance APIs** through **Kafka** and **Spark**")
st.divider()

# Look for actual data partition files, ignoring metadata logs
all_files = glob.glob("./spark_output/part-*.json")

if all_files:
    try:
        # Sort files by modification time so the newest data comes first
        all_files.sort(key=os.path.getmtime, reverse=True)
        
        # Keep only the 15 most recent files to prevent memory choking
        file_paths = all_files[:15]
        
        # Load and combine data
        dataframes = [pd.read_json(f, lines=True, convert_dates=False) for f in file_paths if os.path.getsize(f) > 0]
        
        if dataframes:
            full_df = pd.concat(dataframes, ignore_index=True)
            
            if "Timestamp" in full_df.columns:
                full_df["Timestamp"] = pd.to_datetime(full_df["Timestamp"])
            
            # Feature Engineering: Calculate Total USD Value of each trade
            full_df["Total_USD"] = full_df["Price"] * full_df["Quantity"]
            
            # Sort full dataset chronologically for charts
            full_df = full_df.sort_values(by="Timestamp", ascending=True)
            
            # Latest state extraction
            latest_price = full_df["Price"].iloc[-1]
            total_volume_btc = full_df["Quantity"].sum()
            total_volume_usd = full_df["Total_USD"].sum()
            total_trades = len(full_df)
            
            # ----------------------------------------------------
            # 1. TOP-LEVEL KPI METRICS
            # ----------------------------------------------------
            metric_1, metric_2, metric_3, metric_4 = st.columns(4)
            metric_1.metric("Live BTC Price", f"${latest_price:,.2f} USD")
            metric_2.metric("Total Trades Processed", f"{total_trades}")
            metric_3.metric("Volume (BTC)", f"{total_volume_btc:,.3f} BTC")
            metric_4.metric("Volume (USD Inflow)", f"${total_volume_usd:,.2f}")
            
            st.write("---")
            
            # ----------------------------------------------------
            # 2. CHARTS GRID (ROW 1)
            # ----------------------------------------------------
            row1_col1, row1_col2 = st.columns(2)
            
            with row1_col1:
                st.subheader("1. Real-Time Price Trend")
                fig_price = px.line(full_df, x="Timestamp", y="Price", template="plotly_dark", markers=False)
                fig_price.update_traces(line_color='#00ffcc')
                st.plotly_chart(fig_price, use_container_width=True)
                
                # Dynamic Insight 1
                price_change = full_df["Price"].iloc[-1] - full_df["Price"].iloc[0]
                direction = "📈 UPWARD" if price_change >= 0 else "📉 DOWNWARD"
                st.info(f"**Live Market Insight:** Bitcoin is experiencing a **{direction}** momentum in this batch window, moving by **${abs(price_change):,.2f} USD** from the start of the window.")
            
            with row1_col2:
                st.subheader("2. Cumulative Capital Inflow (USD)")
                full_df["Cumulative_USD"] = full_df["Total_USD"].cumsum()
                fig_cum = px.area(full_df, x="Timestamp", y="Cumulative_USD", template="plotly_dark")
                fig_cum.update_traces(fillcolor='rgba(0, 255, 204, 0.1)', line_color='#00ffcc')
                st.plotly_chart(fig_cum, use_container_width=True)
                
                # Dynamic Insight 2
                avg_trade_value = full_df["Total_USD"].mean()
                st.info(f"**Live Market Insight:** Total liquid capital processed has reached **${total_volume_usd:,.2f}**. The average transaction velocity is injecting **${avg_trade_value:,.2f} USD** per trade into the order book.")

            st.write("---")

            # ----------------------------------------------------
            # 3. CHARTS GRID (ROW 2)
            # ----------------------------------------------------
            row2_col1, row2_col2 = st.columns(2)
            
            with row2_col1:
                st.subheader("3. Market Participant Profiles (Whale Tracker)")
                
                # Segment buyers into financial profile tiers
                conditions = [
                    (full_df["Total_USD"] < 2000),
                    (full_df["Total_USD"] >= 2000) & (full_df["Total_USD"] < 15000),
                    (full_df["Total_USD"] >= 15000) & (full_df["Total_USD"] < 50000),
                    (full_df["Total_USD"] >= 50000)
                ]
                choices = ["Retail (<$2k)", "Professional ($2k-$15k)", "Institutional ($15k-$50k)", "Whale (>$50k)"]
                full_df["Participant_Class"] = np.select(conditions, choices, default="Retail")
                
                class_counts = full_df["Participant_Class"].value_counts().reset_index()
                class_counts.columns = ["Profile", "Count"]
                
                fig_pie = px.pie(class_counts, names="Profile", values="Count", hole=0.4, template="plotly_dark",
                                 color_discrete_sequence=px.colors.sequential.Mint_r)
                st.plotly_chart(fig_pie, use_container_width=True)
                
                # Dynamic Insight 3
                whale_count = len(full_df[full_df["Total_USD"] >= 50000])
                if whale_count > 0:
                    st.warning(f"**Live Market Insight:** 🚨 **Whale Alert!** Detected **{whale_count} high-net-worth orders** exceeding $50,000 USD in this stream. Institutional players are actively adjusting positions.")
                else:
                    st.info("**Live Market Insight:** The current stream window is dominated by retail and mid-tier professional traders. Low institutional volatility detected.")

            with row2_col2:
                st.subheader("4. Price Liquidity & Order Density")
                fig_hist = px.histogram(full_df, x="Price", nbins=15, template="plotly_dark", color_discrete_sequence=['#00cc99'])
                st.plotly_chart(fig_hist, use_container_width=True)
                
                # Dynamic Insight 4
                hist, bin_edges = np.histogram(full_df["Price"], bins=15)
                max_bin_index = hist.argmax()
                dense_price_min = bin_edges[max_bin_index]
                dense_price_max = bin_edges[max_bin_index+1]
                st.info(f"**Live Market Insight:** Order cluster density is highest between **${dense_price_min:,.2f}** and **${dense_price_max:,.2f}**. This range marks the immediate short-term support/resistance zone.")

            st.write("---")
            
            # ----------------------------------------------------
            # 4. RAW RELATIONAL TABLE VIEW
            # ----------------------------------------------------
            st.subheader("5. Processed Streaming Relational View (Latest Batches)")
            display_df = full_df.sort_values(by="Timestamp", ascending=False).head(15)
            st.dataframe(display_df[["Timestamp", "Symbol", "Price", "Quantity", "Total_USD"]], use_container_width=True, hide_index=True)
            
    except Exception as e:
        st.info("Synchronizing data nodes... Aggregating Spark batches.")
        print(f"Pipeline status log: {e}")
else:
    st.warning("⏳ System Initialization: Please execute the Ingestion Engine (`producer.py`) and Spark Processor (`spark_processor.py`) to feed metrics.")
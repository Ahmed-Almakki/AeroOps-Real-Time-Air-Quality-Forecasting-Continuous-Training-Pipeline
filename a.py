import streamlit as st
import pandas as pd
import os
import altair as alt
import numpy as np

# 1. Page Configuration (Must be the first Streamlit command)
st.set_page_config(page_title="Air Quality Dashboard", page_icon="🌤️", layout="wide")


@st.cache_data
def load_data():
    file = "PRSA_Data_Wanshouxigong_20130301-20170228.csv"
    file_name = os.path.join("data", file)

    if not os.path.exists(file_name):
        st.error(f"Cannot find data at {file_name}. Please check the path.")
        return pd.DataFrame()

    df = pd.read_csv(file_name)
    df = df.dropna()

    df['reading_time'] = pd.to_datetime(df[['year', 'month', 'day', 'hour']])
    df = df.drop(columns=['year', 'month', 'day', 'No', 'wd', 'station', 'PM10'])

    df = df.rename(columns={'PM2.5': 'PM2_5'})
    df['PM2_5'] = df['PM2_5'].astype(int)
    df['hour'] = df['hour'].astype(int)
    df['date_only'] = df['reading_time'].dt.date

    np.random.seed(42)
    df['predicted_PM2_5'] = df['PM2_5'] * np.random.uniform(0.7, 1.3, size=len(df))
    df['predicted_PM2_5'] = df['predicted_PM2_5'].astype(int)

    return df

df = load_data()

# Thresholds for colors
moderate = 40
risk = 80


# SIDEBAR CONTROLS
with st.sidebar:
    st.header("⚙️ Dashboard Controls")

    selected_date = st.date_input('🗓️ Choose a Date', value=pd.to_datetime("2016-07-12"))

    st.markdown("---")

    view_mode = st.radio(
        "📊 Select Timeframe View",
        options=["Hourly (Selected Date)", "Daily Trend (Selected Month)"]
    )

    st.markdown("---")
    st.caption("Data source: Wanshouxigong Station")

# MAIN PAGE LAYOUT
st.title("🌤️ Air Quality & Model Prediction Dashboard")
st.markdown("Monitor real-time PM2.5 readings and compare them against predictive models.")

if df.empty:
    st.stop()

if view_mode == "Hourly (Selected Date)":
    st.subheader(f"Hourly Readings for {selected_date}")

    df_filtered = df[df['date_only'] == selected_date].reset_index(drop=True)
    x_axis = 'hour:O'
    x_title = 'Hour of Day'

else:
    selected_month = selected_date.month
    selected_year = selected_date.year
    st.subheader(f"Daily Averages for {selected_date.strftime('%B %Y')}")

    df_filtered = df[(df['reading_time'].dt.month == selected_month) &
                     (df['reading_time'].dt.year == selected_year)]

    df_filtered = df_filtered.groupby('date_only')[['PM2_5', 'predicted_PM2_5']].mean().reset_index()
    x_axis = 'date_only:T'
    x_title = 'Date'


# KPI reading
if not df_filtered.empty:
    avg_actual = int(df_filtered['PM2_5'].mean())
    max_actual = int(df_filtered['PM2_5'].max())
    avg_pred = int(df_filtered['predicted_PM2_5'].mean())

    # Show how much value change between prediction and actaul
    delta_val = avg_actual - avg_pred

    col1, col2, col3 = st.columns(3)
    col1.metric("Average Actual PM2.5", f"{avg_actual} µg/m³")
    col2.metric("Average Predicted PM2.5", f"{avg_pred} µg/m³", delta=f"{delta_val} from model", delta_color="inverse")
    col3.metric("Peak PM2.5 Reading", f"{max_actual} µg/m³")
    st.divider()

    # --- Charts ---
    chart_col1, chart_col2 = st.columns(2)


    with chart_col1:
        st.markdown("**Actual PM2.5 Readings**")

        # 1. Draw a neutral trend line
        trend_line = alt.Chart(df_filtered).mark_line(
            color='#9ca3af',
            strokeWidth=2,
            opacity=0.6
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('PM2_5:Q', title='PM2.5 Level')
        )

        # 2. Draw the colored dots based on thresholds
        colored_points = alt.Chart(df_filtered).mark_circle(
            size=80, # Size of the dots
            opacity=1
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('PM2_5:Q', title='PM2.5 Level'),
            color=alt.Color('PM2_5:Q', legend=alt.Legend(title="Severity")).scale(
                domain=[moderate, risk],
                range=['#10b981', '#eab308', '#ef4444'], # Green, Yellow, Red
                type='threshold'
            ),
            tooltip=[x_axis, 'PM2_5']
        )

        actual_area = alt.Chart(df_filtered).mark_area(
            color="#7e8897",
            opacity=0.15 # Keep it light so it's not overwhelming
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('PM2_5:Q', title='PM2.5 Level')
        )


        # 3. Combine them using the '+' operator
        actual_chart = (trend_line + colored_points + actual_area).properties(
            height=350
        )

        st.altair_chart(actual_chart, use_container_width=True)

    # Chart 2: Model Predictions
    # Chart 2: Model Predictions
    with chart_col2:
        st.markdown("**Model Predictions**")

        # 1. NEW: Soft shaded area to signify a "forecast zone"
        pred_area = alt.Chart(df_filtered).mark_area(
            color="#3b82f6",
            opacity=0.15 # Keep it light so it's not overwhelming
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('predicted_PM2_5:Q', title='Predicted PM2.5 Level')
        )

        # 2. UPGRADED: Added strokeDash=[5, 5] to make the line dashed
        pred_trend_line = alt.Chart(df_filtered).mark_line(
            color="#274391",
            strokeWidth=2,
            strokeDash=[5, 5]
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('predicted_PM2_5:Q')
        )

        # 3. UNCHANGED: Your custom blue colored points
        pred_colored_points = alt.Chart(df_filtered).mark_circle(
            size=100,
            opacity=1
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('predicted_PM2_5:Q'),
            color=alt.Color('predicted_PM2_5:Q', legend=alt.Legend(title="Severity")).scale(
                domain=[moderate, risk],
                range=['#06b6d4', '#a855f7', '#f43f5e'],
                type='threshold'
            ),
            tooltip=[x_axis, 'predicted_PM2_5']
        )

        # 4. Combine all three layers
        pred_chart = (pred_area + pred_trend_line + pred_colored_points).properties(
            height=350
        )

        st.altair_chart(pred_chart, use_container_width=True)

else:
    st.info("No data available for the selected timeframe.")

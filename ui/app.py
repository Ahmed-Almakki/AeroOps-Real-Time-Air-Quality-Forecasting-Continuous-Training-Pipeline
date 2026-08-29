import streamlit as st
import pandas as pd
import altair as alt
import os
from dotenv import load_dotenv
import logging
import sys

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])

def load_data(target_date, mode):
    try:
        logging.info("Start loading data...")

        if mode == "Hourly (Selected Date)":
            query = f"""
                SELECT s.real_output as real, s.reading_time, p.prediction as pred
                FROM {os.getenv("TABLE_NAME")} as s
                JOIN {os.getenv("PREDICTION_TABLE_NAME")} as p
                ON s.reading_time = p.reading_time
                WHERE DATE(s.reading_time) = :sql_date
            """
            params = {"sql_date": target_date.strftime("%Y-%m-%d")}

        else:
            # FIX 1: Let the database group and average the daily data to save memory
            query = f"""
                SELECT DATE(s.reading_time) as reading_time,
                       AVG(s.real_output) as real,
                       AVG(p.prediction) as pred
                FROM {os.getenv("TABLE_NAME")} as s
                JOIN {os.getenv("PREDICTION_TABLE_NAME")} as p
                ON s.reading_time = p.reading_time
                WHERE EXTRACT(MONTH FROM s.reading_time) = :sql_month
                AND EXTRACT(YEAR FROM s.reading_time) = :sql_year
                GROUP BY DATE(s.reading_time)
            """
            params = {"sql_month": target_date.month, "sql_year": target_date.year}

        # FIX 3: Initialize the dataframe before the try/except block to prevent UnboundLocalError
        df = pd.DataFrame()
        try:
            logging.info("Start fetching data from database...")
            # FIX 2: Set ttl=600 to cache results for 10 minutes and prevent database spam
            df = conn.query(query, params=params, ttl=600)
            logging.info("fetch data from database, %s rows found and columns are [%s]", len(df), df.columns)
        except Exception as e:
            logging.error("Failed to fetch from database due to %s", e)

        if df.empty:
            return pd.DataFrame()

        df = df.dropna()
        df['real'] = df['real'].astype(int)

        df['reading_time'] = pd.to_datetime(df['reading_time'])

        # Safe date conversion for Altair/PyArrow
        df['date_only'] = df['reading_time'].dt.normalize()

        # Only extract the hour if we are in Hourly mode
        if mode == "Hourly (Selected Date)":
            df['hour'] = df['reading_time'].dt.hour
            df['hour'] = df['hour'].astype(int)

        return df
    except Exception as e:
        logging.error("Failed to load data due to %s", e)
        return pd.DataFrame()


def sidebar():
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

    return selected_date, view_mode


def view_option(df, selected_date, view_mode):
    try:
        if view_mode == "Hourly (Selected Date)":
            st.subheader(f"Hourly Readings for {selected_date}")

            df_filtered = df[df['date_only'] == pd.to_datetime(selected_date)].reset_index(drop=True)
            x_axis = 'hour:O'
            x_title = 'Hour of Day'

        else:
            st.subheader(f"Daily Averages for {selected_date.strftime('%B %Y')}")

            # Since SQL already filtered and aggregated the data, we just copy it
            df_filtered = df.copy()
            x_axis = 'date_only:T'
            x_title = 'Date'

            if not df_filtered.empty:
                logging.info("View option choosed is for %s the selected reading time is %s", view_mode, df_filtered['date_only'].head(1))

        return {'df_filtered': df_filtered, "x_axis": x_axis, "x_title": x_title}
    except Exception as e:
        logging.error("Failed to choose view type due to %s", e)
        return {'df_filtered': pd.DataFrame(), "x_axis": "", "x_title": ""}


def KPI_reading(df_filtered):
    avg_actual = int(df_filtered['real'].mean())
    max_actual = int(df_filtered['real'].max())
    avg_pred = int(df_filtered['pred'].mean())

    # Show how much value change between prediction and actaul
    delta_val = avg_actual - avg_pred

    col1, col2, col3 = st.columns(3)
    col1.metric("Average Actual PM2.5", f"{avg_actual} µg/m³")
    col2.metric("Average Predicted PM2.5", f"{avg_pred} µg/m³", delta=f"{delta_val} from model", delta_color="inverse")
    col3.metric("Peak PM2.5 Reading", f"{max_actual} µg/m³")

def actual_chart(df_filtered, x_axis, x_title, moderate, risk):
    try:
        st.markdown("**Actual PM2.5 Readings**")

        # 1. Draw a neutral trend line
        trend_line = alt.Chart(df_filtered).mark_line(
            color='#9ca3af',
            strokeWidth=2,
            opacity=0.6
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('real:Q', title='PM2.5 Level')
        )

        # 2. Draw the colored dots based on thresholds
        colored_points = alt.Chart(df_filtered).mark_circle(
            size=80, # Size of the dots
            opacity=1
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('real:Q', title='PM2.5 Level'),
            color=alt.Color('real:Q', legend=alt.Legend(title="Severity")).scale(
                domain=[moderate, risk],
                range=['#10b981', '#eab308', '#ef4444'], # Green, Yellow, Red
                type='threshold'
            ),
            tooltip=[x_axis, 'real']
        )

        actual_area = alt.Chart(df_filtered).mark_area(
            color="#7e8897",
            opacity=0.15 # Keep it light so it's not overwhelming
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('real:Q', title='PM2.5 Level')
        )


        # 3. Combine them using the '+' operator
        chart = (trend_line + colored_points + actual_area).properties(
            height=350
        )

        return chart
    except Exception as e:
        logging.error("Failed to plot the actual chart due to %s", e)


def pred_chart(df_filtered, x_axis, x_title, moderate, risk):
    try:
        st.markdown("**Model Predictions**")

        pred_area = alt.Chart(df_filtered).mark_area(
            color="#3b82f6",
            opacity=0.15
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('pred:Q', title='Predicted PM2.5 Level')
        )

        # 2. UPGRADED: Added strokeDash=[5, 5] to make the line dashed
        pred_trend_line = alt.Chart(df_filtered).mark_line(
            color="#274391",
            strokeWidth=2,
            strokeDash=[5, 5]
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('pred:Q')
        )

        pred_colored_points = alt.Chart(df_filtered).mark_circle(
            size=100,
            opacity=1
        ).encode(
            x=alt.X(x_axis, title=x_title),
            y=alt.Y('pred:Q'),
            color=alt.Color('pred:Q', legend=alt.Legend(title="Severity")).scale(
                domain=[moderate, risk],
                range=['#06b6d4', '#a855f7', '#f43f5e'],
                type='threshold'
            ),
            tooltip=[x_axis, 'pred']
        )

        chart = (pred_area + pred_trend_line + pred_colored_points).properties(
            height=350
        )

        return chart

    except Exception as e:
        logging.error("Failed to plot prediction chart due to %s", e)



if __name__ == "__main__":
    load_dotenv()

    conn = st.connection("postgres-ui", type="sql")

    moderate = 40
    risk = 80

    st.set_page_config(page_title="Air Quality Dashboard", page_icon="🌤️", layout="wide")

    # MAIN PAGE LAYOUT
    st.title("🌤️ Air Quality & Model Prediction Dashboard")
    st.markdown("Monitor real-time PM2.5 readings and compare them against predictive models.")

    selected_date, view_mode = sidebar()

    with st.spinner(f"Fetching data for {selected_date}"):
        df = load_data(selected_date, view_mode)

    if df.empty:
        st.warning("The Selected Date Doesn't have any data. Choose another one.")
        st.stop()

    data = view_option(df, selected_date, view_mode)
    df_filtered = data['df_filtered']
    x_axis = data['x_axis']
    x_title = data['x_title']

    if not df_filtered.empty:
        KPI_reading(df_filtered)

        st.divider()

        chart_col1, chart_col2 = st.columns(2)

        with chart_col1:
            chart_actual = actual_chart(df_filtered, x_axis, x_title, moderate, risk)
            st.altair_chart(chart_actual, width='stretch')

        with chart_col2:
            chart_pred = pred_chart(df_filtered, x_axis, x_title, moderate, risk)
            st.altair_chart(chart_pred, width='stretch')

    else:
        st.info("No data available for the selected timeframe.")

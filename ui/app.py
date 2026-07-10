import streamlit as st
import os


conn = st.connection("postgres-ui", type="sql")

df = conn.query("SELECT * FROM air_pollution limit 3")

for row in df.itertuples():
    st.write(f"rows is {row}")
print('a')

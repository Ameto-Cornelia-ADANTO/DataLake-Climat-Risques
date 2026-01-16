import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime

st.set_page_config(page_title="Dashboard", layout="wide")
st.title("🏠 Dashboard Principal")

# Données simulées
dates = pd.date_range('2024-01-01', periods=30)
df = pd.DataFrame({
    'date': dates,
    'temperature': np.random.normal(15, 3, 30).cumsum()/30 + 15,
    'precipitation': np.random.exponential(2, 30),
    'earthquakes': np.random.poisson(2, 30)
})

# KPIs
col1, col2, col3, col4 = st.columns(4)
col1.metric("🌡️ Température", f"{df['temperature'].mean():.1f}°C")
col2.metric("🌧️ Précipitation", f"{df['precipitation'].sum():.0f} mm")
col3.metric("🌋 Séismes", f"{df['earthquakes'].sum():.0f}")
col4.metric("🚨 Alertes", "2", "Actives")

# Graphiques
col1, col2 = st.columns(2)
with col1:
    fig1 = px.line(df, x='date', y='temperature', title='Évolution Température')
    st.plotly_chart(fig1, use_container_width=True)

with col2:
    fig2 = px.bar(df, x='date', y='earthquakes', title='Activité Sismique')
    st.plotly_chart(fig2, use_container_width=True)
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

st.set_page_config(page_title="NOAA Visualisations", page_icon="📊", layout="wide")

st.title("📊 Visualisations NOAA - Données Météorologiques")

st.info("ℹ️ Cette page montre les visualisations détaillées des données NOAA (National Oceanic and Atmospheric Administration)")

# Charger les données NOAA
@st.cache_data
def load_noaa_data():
    # Données NOAA simulées
    dates = pd.date_range('2024-01-01', periods=100, freq='D')
    stations = ['New York', 'Los Angeles', 'Chicago', 'Miami', 'Seattle', 'Dallas', 'Denver', 'Atlanta']
    
    data = []
    for date in dates:
        for station in stations:
            # Variation saisonnière de la température
            seasonal_temp = 5 * np.sin(date.dayofyear/365 * 2 * np.pi)
            
            data.append({
                'Date': date,
                'Station': station,
                'Temperature_C': np.random.normal(15, 8) + seasonal_temp,
                'Humidity_pct': np.random.uniform(30, 90),
                'Wind_Speed_kmh': np.random.exponential(15),
                'Pressure_hPa': np.random.normal(1013, 10),
                'Precipitation_mm': np.random.exponential(2)
            })
    
    df = pd.DataFrame(data)
    
    # Ajouter des coordonnées géographiques pour chaque station
    station_coords = {
        'New York': (40.7128, -74.0060, 10),
        'Los Angeles': (34.0522, -118.2437, 71),
        'Chicago': (41.8781, -87.6298, 182),
        'Miami': (25.7617, -80.1918, 2),
        'Seattle': (47.6062, -122.3321, 50),
        'Dallas': (32.7767, -96.7970, 131),
        'Denver': (39.7392, -104.9903, 1609),
        'Atlanta': (33.7490, -84.3880, 320)
    }
    
    df['Latitude'] = df['Station'].map(lambda x: station_coords[x][0])
    df['Longitude'] = df['Station'].map(lambda x: station_coords[x][1])
    df['Altitude_m'] = df['Station'].map(lambda x: station_coords[x][2])
    
    return df

df_noaa = load_noaa_data()

# Afficher les métriques principales
st.subheader("📈 Métriques Globales")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("📊 Enregistrements", f"{len(df_noaa):,}")
with col2:
    st.metric("🏙️ Stations", df_noaa['Station'].nunique())
with col3:
    avg_temp = df_noaa['Temperature_C'].mean()
    st.metric("🌡️ Température moyenne", f"{avg_temp:.1f}°C")
with col4:
    avg_humidity = df_noaa['Humidity_pct'].mean()
    st.metric("💧 Humidité moyenne", f"{avg_humidity:.0f}%")

# Onglets pour différentes visualisations
tab1, tab2, tab3 = st.tabs(["📊 Graphiques", "🗺️ Carte", "📋 Données"])

with tab1:
    st.subheader("🔍 Filtres")
    
    col1, col2 = st.columns(2)
    
    with col1:
        selected_stations = st.multiselect(
            "Sélectionner les stations",
            df_noaa['Station'].unique(),
            default=df_noaa['Station'].unique()[:3]
        )
    
    with col2:
        temp_range = st.slider(
            "Plage de température (°C)",
            float(df_noaa['Temperature_C'].min()),
            float(df_noaa['Temperature_C'].max()),
            (-10.0, 30.0)
        )
    
    # Filtrer les données
    filtered_df = df_noaa[
        (df_noaa['Station'].isin(selected_stations)) &
        (df_noaa['Temperature_C'] >= temp_range[0]) &
        (df_noaa['Temperature_C'] <= temp_range[1])
    ]
    
    if filtered_df.empty:
        st.warning("⚠️ Aucune donnée ne correspond aux filtres sélectionnés")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            # Scatter plot
            st.markdown("#### 📍 Température vs Humidité")
            fig_scatter = px.scatter(
                filtered_df,
                x='Temperature_C',
                y='Humidity_pct',
                color='Station',
                size='Wind_Speed_kmh',
                hover_data=['Date', 'Pressure_hPa', 'Precipitation_mm'],
                title='Relation Température-Humidité'
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        with col2:
            # Box plot
            st.markdown("#### 📦 Distribution des Températures")
            fig_box = px.box(
                filtered_df,
                x='Station',
                y='Temperature_C',
                color='Station',
                title='Distribution par Station',
                points='all'
            )
            st.plotly_chart(fig_box, use_container_width=True)
        
        # Graphique temporel
        st.markdown("#### 📈 Évolution Temporelle")
        
        # Sélection de la station pour le graphique temporel
        selected_station_ts = st.selectbox(
            "Sélectionner une station pour la série temporelle",
            filtered_df['Station'].unique(),
            key="station_ts"
        )
        
        station_data = filtered_df[filtered_df['Station'] == selected_station_ts].sort_values('Date')
        
        if not station_data.empty:
            # Graphique avec plusieurs axes Y (VERSION CORRIGÉE)
            fig_time = go.Figure()
            
            # Température
            fig_time.add_trace(go.Scatter(
                x=station_data['Date'],
                y=station_data['Temperature_C'],
                mode='lines+markers',
                name='Température (°C)',
                line=dict(color='red', width=2)
            ))
            
            # Humidité
            fig_time.add_trace(go.Scatter(
                x=station_data['Date'],
                y=station_data['Humidity_pct'],
                mode='lines+markers',
                name='Humidité (%)',
                line=dict(color='blue', width=2)
            ))
            
            # CORRECTION : Configuration correcte du layout
            fig_time.update_layout(
                title=f'Évolution Temporelle - Station {selected_station_ts}',
                xaxis_title='Date',
                yaxis=dict(
                    title='Température (°C)',
                    titlefont=dict(color='red')
                ),
                yaxis2=dict(
                    title='Humidité (%)',
                    titlefont=dict(color='blue'),
                    overlaying='y',
                    side='right'
                ),
                height=400,
                hovermode='x unified'
            )
            
            # Assigner le deuxième axe Y à la trace d'humidité
            fig_time.update_traces(yaxis='y2', selector=dict(name='Humidité (%)'))
            
            st.plotly_chart(fig_time, use_container_width=True)
        
        # Matrice de corrélation
        st.markdown("#### 🔗 Matrice de Corrélation")
        
        # Sélectionner uniquement les colonnes numériques
        numeric_cols = ['Temperature_C', 'Humidity_pct', 'Wind_Speed_kmh', 'Pressure_hPa', 'Precipitation_mm']
        corr_matrix = filtered_df[numeric_cols].corr()
        
        # Créer les labels en français
        french_labels = {
            'Temperature_C': 'Température (°C)',
            'Humidity_pct': 'Humidité (%)',
            'Wind_Speed_kmh': 'Vitesse vent (km/h)',
            'Pressure_hPa': 'Pression (hPa)',
            'Precipitation_mm': 'Précipitation (mm)'
        }
        
        # Renommer les index et colonnes
        corr_matrix_renamed = corr_matrix.rename(
            index=french_labels,
            columns=french_labels
        )
        
        fig_corr = px.imshow(
            corr_matrix_renamed,
            text_auto=True,
            aspect='auto',
            color_continuous_scale='RdBu',
            title='Corrélations entre Variables'
        )
        st.plotly_chart(fig_corr, use_container_width=True)

with tab2:
    st.subheader("🗺️ Carte des Stations Météo")
    
    # Données géographiques des stations
    stations_info = df_noaa.groupby('Station').agg({
        'Latitude': 'first',
        'Longitude': 'first',
        'Altitude_m': 'first',
        'Temperature_C': 'mean',
        'Humidity_pct': 'mean'
    }).reset_index()
    
    # Ajouter une colonne de taille pour la carte
    stations_info['Size'] = 20  # Taille fixe pour tous les points
    
    # Afficher la carte SIMPLIFIÉE
    try:
        # Version simplifiée sans paramètres problématiques
        st.map(stations_info[['Latitude', 'Longitude']].dropna())
    except Exception as e:
        st.error(f"Erreur avec la carte : {str(e)[:100]}")
        # Solution de secours
        st.write("📍 Emplacements des stations :")
        st.dataframe(stations_info[['Station', 'Latitude', 'Longitude', 'Altitude_m']])
    
    # Table d'information
    st.subheader("📋 Informations des Stations")
    
    # Préparer les données pour l'affichage
    display_cols = ['Station', 'Latitude', 'Longitude', 'Altitude_m', 
                   'Temperature_C', 'Humidity_pct']
    stations_display = stations_info[display_cols].copy()
    
    # Arrondir les valeurs
    stations_display['Temperature_C'] = stations_display['Temperature_C'].round(1)
    stations_display['Humidity_pct'] = stations_display['Humidity_pct'].round(1)
    stations_display['Latitude'] = stations_display['Latitude'].round(4)
    stations_display['Longitude'] = stations_display['Longitude'].round(4)
    
    st.dataframe(stations_display, use_container_width=True)
    
    # Graphique de répartition
    st.subheader("📊 Répartition des Stations")
    
    fig_bar = px.bar(
        stations_info,
        x='Station',
        y='Temperature_C',
        color='Humidity_pct',
        title='Température Moyenne par Station',
        color_continuous_scale='Bluered',
        labels={
            'Temperature_C': 'Température moyenne (°C)',
            'Humidity_pct': 'Humidité moyenne (%)',
            'Station': 'Station'
        }
    )
    st.plotly_chart(fig_bar, use_container_width=True)

with tab3:
    st.subheader("📋 Données Brutes")
    
    # Options d'affichage
    col1, col2 = st.columns(2)
    with col1:
        rows_to_show = st.selectbox("Nombre de lignes à afficher", [10, 25, 50, 100, 500])
    with col2:
        sort_by = st.selectbox("Trier par", df_noaa.columns.tolist())
    
    # Afficher les données
    st.dataframe(
        df_noaa.sort_values(sort_by).head(rows_to_show),
        use_container_width=True
    )
    
    # Statistiques descriptives
    st.subheader("📊 Statistiques Descriptives")
    
    # Sélectionner uniquement les colonnes numériques
    numeric_cols = df_noaa.select_dtypes(include=[np.number]).columns.tolist()
    stats_df = df_noaa[numeric_cols].describe().round(2)
    
    # Renommer les colonnes pour plus de clarté
    rename_dict = {
        'Temperature_C': 'Température (°C)',
        'Humidity_pct': 'Humidité (%)',
        'Wind_Speed_kmh': 'Vent (km/h)',
        'Pressure_hPa': 'Pression (hPa)',
        'Precipitation_mm': 'Précipitation (mm)',
        'Altitude_m': 'Altitude (m)'
    }
    
    stats_df_renamed = stats_df.rename(columns=rename_dict)
    st.dataframe(stats_df_renamed, use_container_width=True)
    
    # Distribution des variables
    st.subheader("📈 Distribution des Variables")
    
    selected_variable = st.selectbox(
        "Sélectionner une variable à visualiser",
        ['Temperature_C', 'Humidity_pct', 'Wind_Speed_kmh', 'Pressure_hPa', 'Precipitation_mm'],
        format_func=lambda x: {
            'Temperature_C': 'Température',
            'Humidity_pct': 'Humidité',
            'Wind_Speed_kmh': 'Vitesse du vent',
            'Pressure_hPa': 'Pression atmosphérique',
            'Precipitation_mm': 'Précipitation'
        }[x]
    )
    
    # Histogramme
    fig_hist = px.histogram(
        df_noaa,
        x=selected_variable,
        nbins=30,
        title=f'Distribution de {selected_variable}',
        color='Station',
        marginal='box'
    )
    
    # Mettre à jour les labels
    variable_labels = {
        'Temperature_C': 'Température (°C)',
        'Humidity_pct': 'Humidité (%)',
        'Wind_Speed_kmh': 'Vitesse du vent (km/h)',
        'Pressure_hPa': 'Pression atmosphérique (hPa)',
        'Precipitation_mm': 'Précipitation (mm)'
    }
    
    fig_hist.update_layout(
        xaxis_title=variable_labels.get(selected_variable, selected_variable),
        yaxis_title='Fréquence'
    )
    
    st.plotly_chart(fig_hist, use_container_width=True)
    
    # Bouton de téléchargement
    csv_data = df_noaa.to_csv(index=False)
    st.download_button(
        label="📥 Télécharger les données (CSV)",
        data=csv_data,
        file_name=f"noaa_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv",
        use_container_width=True
    )

# Informations sur les données
with st.expander("ℹ️ À propos des données NOAA", expanded=False):
    st.markdown("""
    ## 🌐 Source des données: NOAA (National Oceanic and Atmospheric Administration)
    
    ### 📊 Types de données collectées:
    - **Température de l'air** (°C) - Mesures horaires et quotidiennes
    - **Humidité relative** (%) - Pourcentage d'humidité dans l'air
    - **Vitesse du vent** (km/h) - Mesures instantanées et moyennes
    - **Pression atmosphérique** (hPa) - Niveau de pression au sol
    - **Précipitations** (mm) - Cumul journalier
    
    ### 🏙️ Stations de mesure:
    - **8 stations principales** aux États-Unis
    - **Couverture nationale** représentative
    - **Données historiques** disponibles
    
    ### 🔧 Qualité des données:
    - **Vérifiées et validées** par la NOAA
    - **Calibrées régulièrement**
    - **Format standardisé** pour l'analyse
    
    ### 🎯 Utilisations principales:
    - Analyse climatique à long terme
    - Prévisions météorologiques
    - Recherche sur le changement climatique
    - Évaluation des risques naturels
    
    ### 📅 Période couverte:
    - **100 derniers jours** (données simulées)
    - **Fréquence:** Quotidienne
    - **Résolution:** Données moyennes journalières
    
    ### 🗺️ Localisation des stations:
    1. **New York** - Zone urbaine côtière
    2. **Los Angeles** - Zone urbaine méditerranéenne
    3. **Chicago** - Zone urbaine continentale
    4. **Miami** - Zone côtière tropicale
    5. **Seattle** - Zone côtière pluvieuse
    6. **Dallas** - Zone continentale sèche
    7. **Denver** - Zone montagneuse
    8. **Atlanta** - Zone urbaine subtropicale
    
    ### ⚠️ Limitations:
    - Données simulées pour cette démonstration
    - En production: données temps réel de l'API NOAA
    - Fréquence de mise à jour: horaire
    - Résolution spatiale limitée aux stations principales
    
    ### 🔍 Méthodologie:
    - Collecte automatisée des données
    - Validation des capteurs
    - Correction des biais instrumentaux
    - Normalisation des formats
    - Archivage sécurisé
    
    ### 📈 Analyses disponibles:
    - Tendances temporelles
    - Comparaisons inter-stations
    - Corrélations entre variables
    - Visualisations interactives
    - Export de données brutes
    
    ### 🛠️ Technologies utilisées:
    - API REST pour l'acquisition
    - Base de données temporelles
    - Traitement en temps réel
    - Visualisation interactive
    
    Pour plus d'informations sur les données réelles de la NOAA:
    [https://www.noaa.gov/weather-climate](https://www.noaa.gov/weather-climate)
    """)
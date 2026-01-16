# ============================================
# DataLake Climat & Risques Naturels
# Dashboard Streamlit - Version Complète
# ============================================
import streamlit as st
import pandas as pd
import numpy as np

# Désactiver certains caches problématiques
st.set_option('client.caching', False)
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
from datetime import datetime, timedelta
import time
import os

# ============================================
# CONFIGURATION DE LA PAGE
# ============================================

st.set_page_config(
    page_title="DataLake Climat & Risques Naturels",
    page_icon="🌍",
    layout="wide"
)

# ============================================
# STYLES CSS
# ============================================

st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1E88E5;
        text-align: center;
        padding: 1rem;
        background: linear-gradient(90deg, #1E88E5, #4CAF50);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: bold;
        margin-bottom: 1rem;
    }
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 15px;
        color: white;
        text-align: center;
        box-shadow: 0 6px 12px rgba(0,0,0,0.15);
        margin: 0.5rem;
        transition: transform 0.3s;
    }
    .kpi-card:hover {
        transform: translateY(-5px);
    }
    .kpi-card h2 {
        font-size: 2.2rem;
        margin: 0.5rem 0;
        font-weight: bold;
    }
    .kpi-card p {
        margin: 0;
        font-size: 0.9rem;
        opacity: 0.9;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: #f0f2f6;
        border-radius: 5px 5px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] {
        background-color: #4CAF50;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# ============================================
# TITRE PRINCIPAL
# ============================================

st.markdown('<h1 class="main-header">🌍 DataLake Climat & Risques Naturels</h1>', unsafe_allow_html=True)
st.markdown("### **NOAA (Météo & Climat)** + **USGS (Risques Naturels)**")

# ============================================
# MENU LATÉRAL
# ============================================

menu = st.sidebar.radio(
    "📊 Navigation",
    ["🏠 Dashboard", "📈 Visualisations", "🚨 Alertes", "📁 HDFS Explorer", "⚙️ Administration", "🏗️ Architecture"]
)

# Vérification des services Docker
with st.sidebar.expander("🔧 État des Services"):
    try:
        import docker
        client = docker.from_env()
        containers = client.containers.list()
        st.success(f"✅ {len(containers)} conteneurs actifs")
        for container in containers[:5]:
            status = "🟢" if container.status == "running" else "🟡"
            st.write(f"{status} {container.name[:20]}...")
    except:
        st.info("ℹ️ Docker Desktop non détecté (mode simulation)")

# ============================================
# DONNÉES SIMULÉES
# ============================================

@st.cache_data(ttl=300)  # Cache 5 minutes
def generate_data():
    """Génère des données simulées pour le dashboard"""
    dates = pd.date_range('2024-01-01', periods=100)
    return pd.DataFrame({
        'date': dates,
        'temperature': np.random.normal(15, 5, 100).cumsum()/100 + 15,
        'precipitation': np.random.exponential(2, 100),
        'earthquakes': np.random.poisson(2, 100),
        'region': np.random.choice(['California', 'Alaska', 'Hawaii', 'Texas'], 100)
    })

# ============================================
# PAGE 1 : DASHBOARD PRINCIPAL
# ============================================

if menu == "🏠 Dashboard":
    
    # Titre
    st.header("📊 Tableau de Bord Principal")
    
    # Données
    df = generate_data()
    
    # ========== KPIs ==========
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("""
        <div class="kpi-card">
            <p>🌡️ Température</p>
            <h2>15.2°C</h2>
            <p>+0.5°C</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
        <div class="kpi-card">
            <p>🌧️ Précipitation</p>
            <h2>42 mm</h2>
            <p>-10%</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
        <div class="kpi-card">
            <p>🌋 Séismes</p>
            <h2>24</h2>
            <p>+3</p>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown("""
        <div class="kpi-card">
            <p>🚨 Alertes</p>
            <h2>2</h2>
            <p>Actives</p>
        </div>
        """, unsafe_allow_html=True)
    
    # ========== GRAPHIQUES ==========
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Graphique température
        fig_temp = px.line(df, x='date', y='temperature', 
                          title='📈 Évolution des Températures',
                          labels={'temperature': 'Température (°C)', 'date': 'Date'},
                          line_shape='spline')
        fig_temp.update_layout(
            height=400,
            hovermode='x unified',
            plot_bgcolor='rgba(240, 242, 246, 0.8)'
        )
        st.plotly_chart(fig_temp, use_container_width=True)
    
    with col2:
        # Graphique activité sismique
        region_counts = df.groupby('region')['earthquakes'].sum().reset_index()
        fig_seismic = px.bar(region_counts,
                            x='region', y='earthquakes',
                            title='🌋 Activité Sismique par Région',
                            color='region',
                            color_discrete_sequence=px.colors.qualitative.Set3)
        fig_seismic.update_layout(
            height=400,
            xaxis_title="Région",
            yaxis_title="Nombre de séismes",
            plot_bgcolor='rgba(240, 242, 246, 0.8)'
        )
        st.plotly_chart(fig_seismic, use_container_width=True)
    
    # ========== CARTE ==========
    st.markdown("---")
    st.subheader("📍 Carte des Stations et Séismes")
    
    # Données pour la carte
    df_map = pd.DataFrame({
        'lat': np.random.uniform(30, 50, 20),
        'lon': np.random.uniform(-130, -60, 20),
        'type': np.random.choice(['Station NOAA', 'Séisme USGS'], 20),
        'size': np.random.randint(10, 50, 20)
    })
    
    # Ajouter des couleurs au format hexadécimal
    df_map['color'] = df_map['type'].map({
        'Station NOAA': '#FF0000',  # Rouge
        'Séisme USGS': '#0000FF'    # Bleu
    })
    
    # Carte simplifiée
    try:
        # Version simplifiée
        st.map(df_map[['lat', 'lon', 'color']].dropna())
        
        # Légende
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("🔴 **Stations NOAA** - Données météorologiques")
        with col2:
            st.markdown("🔵 **Séismes USGS** - Activité sismique")
            
    except Exception as e:
        st.error(f"Erreur avec la carte : {str(e)[:100]}")
        # Afficher les données à la place
        st.dataframe(df_map.head(10))

# ============================================
# PAGE 2 : VISUALISATIONS
# ============================================

elif menu == "📈 Visualisations":
    
    st.header("📊 Visualisations Avancées")
    
    # Onglets
    tab1, tab2, tab3 = st.tabs(["📊 NOAA", "🌋 USGS", "🔗 Corrélations"])
    
    # ========== TAB 1 : NOAA ==========
    with tab1:
        st.subheader("Données Météorologiques NOAA")
        
        # Données NOAA simulées
        df_noaa = pd.DataFrame({
            'Station': ['New York', 'Los Angeles', 'Chicago', 'Miami'] * 25,
            'Temperature (°C)': np.random.normal(20, 10, 100),
            'Humidity (%)': np.random.uniform(30, 90, 100),
            'Wind Speed (km/h)': np.random.exponential(15, 100),
            'Pressure (hPa)': np.random.normal(1013, 10, 100)
        })
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Scatter plot SANS trendline pour éviter l'erreur statsmodels
            fig_scatter = px.scatter(df_noaa, 
                                    x='Temperature (°C)', 
                                    y='Humidity (%)', 
                                    color='Station', 
                                    size='Wind Speed (km/h)',
                                    title='Température vs Humidité par Station',
                                    hover_data=['Pressure (hPa)'])
            fig_scatter.update_layout(height=450)
            st.plotly_chart(fig_scatter, use_container_width=True)
        
        with col2:
            # Box plot
            fig_box = px.box(df_noaa, 
                            x='Station', 
                            y='Temperature (°C)',
                            title='Distribution des Températures par Station',
                            color='Station')
            fig_box.update_layout(height=450)
            st.plotly_chart(fig_box, use_container_width=True)
        
        # Heatmap
        st.subheader("📊 Heatmap des Variables Météo")
        corr_matrix = df_noaa[['Temperature (°C)', 'Humidity (%)', 'Wind Speed (km/h)', 'Pressure (hPa)']].corr()
        fig_heat = px.imshow(corr_matrix, 
                            text_auto=True, 
                            aspect="auto",
                            title='Matrice de Corrélation des Variables Météo',
                            color_continuous_scale='RdBu')
        st.plotly_chart(fig_heat, use_container_width=True)
    
    # ========== TAB 2 : USGS ==========
    with tab2:
        st.subheader("Données Sismiques USGS")
        
        # Données USGS simulées
        df_usgs = pd.DataFrame({
            'Magnitude': np.random.uniform(2, 8, 50),
            'Depth (km)': np.random.uniform(1, 100, 50),
            'Region': np.random.choice(['California', 'Alaska', 'Hawaii', 'Nevada'], 50),
            'Latitude': np.random.uniform(30, 50, 50),
            'Longitude': np.random.uniform(-130, -60, 50)
        })
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogramme des magnitudes
            fig_hist = px.histogram(df_usgs, 
                                   x='Magnitude', 
                                   nbins=20,
                                   title='Distribution des Magnitudes',
                                   color='Region',
                                   marginal="box")
            fig_hist.update_layout(height=450)
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Box plot par région
            fig_box = px.box(df_usgs, 
                            x='Region', 
                            y='Magnitude',
                            title='Magnitudes par Région',
                            color='Region')
            fig_box.update_layout(height=450)
            st.plotly_chart(fig_box, use_container_width=True)
        
        # Carte 3D
        st.subheader("🗺️ Visualisation 3D")
        fig_3d = px.scatter_3d(df_usgs,
                              x='Longitude',
                              y='Latitude', 
                              z='Depth (km)',
                              color='Magnitude',
                              size='Magnitude',
                              title='Localisation 3D des Séismes',
                              hover_name='Region')
        st.plotly_chart(fig_3d, use_container_width=True)
    
    # ========== TAB 3 : CORRÉLATIONS ==========
    with tab3:
        st.subheader("Corrélations NOAA-USGS")
        
        # Données corrélées simulées
        dates = pd.date_range('2024-01-01', periods=100)
        df_corr = pd.DataFrame({
            'Date': dates,
            'Temperature': np.random.normal(20, 5, 100),
            'Earthquake_Frequency': np.random.poisson(3, 100),
            'Precipitation': np.random.exponential(5, 100),
            'Seismic_Energy': np.random.exponential(10, 100)
        })
        
        # Matrice de corrélation
        corr_matrix = df_corr[['Temperature', 'Earthquake_Frequency', 'Precipitation', 'Seismic_Energy']].corr()
        
        fig_corr = px.imshow(corr_matrix, 
                            text_auto=True, 
                            aspect="auto",
                            title='📊 Matrice de Corrélation NOAA-USGS',
                            color_continuous_scale='RdBu_r',
                            labels=dict(color="Corrélation"))
        fig_corr.update_layout(height=500)
        st.plotly_chart(fig_corr, use_container_width=True)
        
        # Insights
        st.info("""
        ## 🔍 Insights des Corrélations
        
        **📈 Température ↔ Fréquence sismique:** 
        - Corrélation faible (r ≈ 0.15)
        - À étudier plus en détail
        
        **🌧️ Précipitation ↔ Énergie sismique:** 
        - Corrélation négative modérée (r ≈ -0.32)
        - Les périodes de fortes pluies semblent coïncider avec une activité sismique réduite
        
        **🔍 Patterns saisonniers détectés:**
        - Augmentation des séismes en été
        - Corrélation avec la sécheresse
        """)

# ============================================
# PAGE 3 : ALERTES
# ============================================

elif menu == "🚨 Alertes":
    
    st.header("🚨 Système d'Alertes Temps Réel")
    
    # ========== SIMULATION D'ALERTE ==========
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.subheader("Simulation d'Alerte")
        
        if st.button("🔄 Simuler nouvelle alerte", type="primary", use_container_width=True):
            # Types d'alertes
            alert_types = [
                {"emoji": "🌋", "message": "Séisme magnitude 6.2", "location": "California", "source": "USGS"},
                {"emoji": "🌪️", "message": "Tempête tropicale", "location": "Texas", "source": "NOAA"},
                {"emoji": "🌡️", "message": "Vague de chaleur extrême", "location": "Arizona", "source": "NOAA"},
                {"emoji": "🌧️", "message": "Inondations majeures", "location": "Floride", "source": "NOAA"}
            ]
            
            # Choix aléatoire
            alert = np.random.choice(alert_types)
            
            # Affichage de l'alerte
            with st.chat_message("warning"):
                st.warning(f"{alert['emoji']} **ALERTE : {alert['message']}**")
                st.write(f"📍 **Localisation :** {alert['location']}")
                st.write(f"📡 **Source :** {alert['source']}")
                st.write(f"⏰ **Timestamp :** {datetime.now().strftime('%H:%M:%S')}")
            
            # Confetti pour l'effet visuel
            st.balloons()
    
    with col2:
        st.subheader("Statistiques")
        st.metric("Alertes actives", "2")
        st.metric("Dernière alerte", "15 min")
        st.metric("Taux d'alertes", "3/jour")
    
    # ========== HISTORIQUE DES ALERTES ==========
    st.markdown("---")
    st.subheader("📋 Historique des Alertes")
    
    # Données d'historique
    alerts_data = pd.DataFrame({
        'Timestamp': pd.date_range('2024-01-14', periods=10, freq='H'),
        'Type': np.random.choice(['Séisme', 'Tempête', 'Inondation', 'Chaleur'], 10),
        'Severity': np.random.choice(['Faible', 'Modérée', 'Élevée', 'Critique'], 10),
        'Region': np.random.choice(['Californie', 'Alaska', 'Texas', 'Floride'], 10),
        'Status': np.random.choice(['Active', 'Résolue', 'En cours'], 10)
    })
    
    # Style conditionnel
    def severity_color(val):
        if val == 'Critique':
            return 'background-color: #ffcccc'
        elif val == 'Élevée':
            return 'background-color: #ffebcc'
        elif val == 'Modérée':
            return 'background-color: #ffffcc'
        else:
            return 'background-color: #ccffcc'
    
    st.dataframe(
        alerts_data.style.applymap(severity_color, subset=['Severity']),
        use_container_width=True,
        height=300
    )
    
    # Graphique des alertes par type
    st.subheader("📊 Répartition des Alertes")
    alert_counts = alerts_data['Type'].value_counts().reset_index()
    alert_counts.columns = ['Type', 'Count']
    
    fig_alerts = px.pie(alert_counts, 
                       values='Count', 
                       names='Type',
                       title='Répartition des Alertes par Type',
                       hole=0.3)
    st.plotly_chart(fig_alerts, use_container_width=True)

# ============================================
# PAGE 4 : HDFS EXPLORER
# ============================================

elif menu == "📁 HDFS Explorer":
    
    st.header("📁 Explorateur HDFS")
    
    # Information de connexion
    st.info("""
    **Connexion HDFS active** ✅
    - **Namenode:** namenode:9000
    - **Chemin racine:** /hadoop-climate-risk
    - **Mode:** Simulation (données fictives)
    """)
    
    # ========== STRUCTURE HDFS ==========
    st.subheader("🏗️ Structure du DataLake")
    
    # Structure simulée
    hdfs_structure = {
        "📁 /hadoop-climate-risk": {
            "📁 raw (Données brutes)": {
                "📁 noaa": [
                    "noaa_20240114.parquet (1.2 GB)",
                    "noaa_20240113.parquet (1.1 GB)",
                    "noaa_20240112.parquet (1.3 GB)"
                ],
                "📁 usgs": [
                    "earthquakes_2024.parquet (850 MB)",
                    "seismic_data.parquet (720 MB)",
                    "usgs_latest.json (150 MB)"
                ]
            },
            "📁 silver (Nettoyées)": {
                "📁 cleaned": [
                    "noaa_cleaned.parquet (980 MB)",
                    "usgs_cleaned.parquet (680 MB)"
                ],
                "📁 normalized": [
                    "data_normalized.parquet (1.5 GB)"
                ]
            },
            "📁 gold (Agrégées)": {
                "📁 aggregates": [
                    "daily_aggregates.parquet (320 MB)",
                    "monthly_trends.parquet (180 MB)",
                    "weekly_report.parquet (95 MB)"
                ],
                "📁 reports": [
                    "climate_report.json (45 MB)",
                    "seismic_analysis.json (38 MB)",
                    "correlation_study.json (52 MB)"
                ]
            },
            "📁 alerts (Streaming)": {
                "📁 kafka": [
                    "climate-alerts.parquet (210 MB)",
                    "alerts_stream.parquet (185 MB)"
                ],
                "📁 processed": [
                    "alerts_processed.parquet (120 MB)",
                    "anomalies_detected.parquet (95 MB)"
                ]
            }
        }
    }
    
    # Fonction récursive pour afficher l'arborescence
    def display_tree(structure, level=0):
        for key, value in structure.items():
            if isinstance(value, dict):
                with st.expander(f"{'  ' * level}{key}"):
                    display_tree(value, level + 1)
            else:
                for item in value:
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        st.write(f"{'  ' * (level + 1)}📄 {item}")
                    with col2:
                        if st.button("Aperçu", key=f"view_{item}"):
                            st.info(f"Aperçu de {item} - Données simulées")
    
    display_tree(hdfs_structure)
    
    # ========== STATISTIQUES HDFS ==========
    st.markdown("---")
    st.subheader("📊 Statistiques HDFS")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Fichiers totaux", "42", "+3")
    
    with col2:
        st.metric("Taille totale", "2.4 GB", "+0.3 GB")
    
    with col3:
        st.metric("Dernière mise à jour", "15:30")
    
    with col4:
        st.metric("Espace utilisé", "78%")
    
    # ========== APERÇU DES DONNÉES ==========
    st.subheader("👁️ Aperçu des Données")
    
    file_to_preview = st.selectbox(
        "Sélectionner un fichier à prévisualiser",
        [
            "noaa_20240114.parquet",
            "earthquakes_2024.parquet", 
            "daily_aggregates.parquet",
            "climate_report.json"
        ]
    )
    
    if st.button("📖 Afficher l'aperçu"):
        # Données simulées selon le type de fichier
        if "noaa" in file_to_preview:
            sample_data = pd.DataFrame({
                'date': pd.date_range('2024-01-01', periods=10),
                'station_id': ['NYC001', 'LAX002', 'CHI003', 'MIA004', 'SEA005'] * 2,
                'temperature': np.random.normal(15, 5, 10),
                'precipitation': np.random.exponential(2, 10),
                'humidity': np.random.uniform(30, 90, 10)
            })
        elif "earthquake" in file_to_preview:
            sample_data = pd.DataFrame({
                'timestamp': pd.date_range('2024-01-01', periods=10, freq='H'),
                'magnitude': np.random.uniform(2, 8, 10),
                'latitude': np.random.uniform(30, 50, 10),
                'longitude': np.random.uniform(-130, -60, 10),
                'depth_km': np.random.uniform(1, 100, 10)
            })
        else:
            sample_data = pd.DataFrame({
                'metric': ['Température moyenne', 'Précipitation totale', 'Séismes count'],
                'value': [15.2, 42.5, 24],
                'unit': ['°C', 'mm', 'count']
            })
        
        st.dataframe(sample_data, use_container_width=True)
        st.info(f"📄 Fichier: {file_to_preview} | 📊 Lignes: {len(sample_data)} | 📐 Colonnes: {len(sample_data.columns)}")

# ============================================
# PAGE 5 : ADMINISTRATION
# ============================================

elif menu == "⚙️ Administration":
    
    st.header("⚙️ Administration du DataLake")
    
    # Onglets
    tab1, tab2, tab3 = st.tabs(["📥 Ingestion", "🔧 Traitement", "📤 Export"])
    
    # ========== TAB 1 : INGESTION ==========
    with tab1:
        st.subheader("Ingestion des Données")
        
        col1, col2 = st.columns(2)
        
        # Bouton NOAA
        with col1:
            if st.button("🚀 Lancer ingestion NOAA", 
                        use_container_width=True,
                        help="Collecte des données météo depuis l'API NOAA"):
                with st.spinner("Connexion à l'API NOAA..."):
                    # Barre de progression
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for i in range(100):
                        time.sleep(0.02)
                        progress_bar.progress(i + 1)
                        
                        # Mise à jour du statut
                        if i < 30:
                            status_text.text("🔌 Connexion à l'API...")
                        elif i < 60:
                            status_text.text("📥 Téléchargement des données...")
                        elif i < 90:
                            status_text.text("💾 Écriture vers HDFS...")
                        else:
                            status_text.text("✅ Finalisation...")
                    
                    # Résultats
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    st.success("✅ 1,250 enregistrements NOAA ingérés")
                    st.info(f"""
                    **Chemin HDFS:** `/hadoop-climate-risk/raw/noaa/noaa_{timestamp}.parquet`
                    
                    **Détails:**
                    - Format: Parquet (compressé)
                    - Taille: ~45 MB
                    - Période: Derniers 30 jours
                    - Stations: 15 stations météo
                    """)
                    
                    # Aperçu des données
                    with st.expander("📋 Aperçu des données ingérées"):
                        sample_df = pd.DataFrame({
                            'date': pd.date_range('2024-01-01', periods=5),
                            'station': ['NYC001', 'LAX002', 'CHI003', 'MIA004', 'SEA005'],
                            'temperature': [15.2, 18.5, 12.3, 24.1, 10.8],
                            'humidity': [65, 42, 78, 85, 55],
                            'wind_speed': [12.3, 8.7, 15.2, 5.4, 20.1]
                        })
                        st.dataframe(sample_df)
        
        # Bouton USGS
        with col2:
            if st.button("🚀 Lancer ingestion USGS", 
                        use_container_width=True,
                        help="Collecte des données sismiques depuis l'API USGS"):
                with st.spinner("Connexion à l'API USGS..."):
                    # Barre de progression
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    for i in range(100):
                        time.sleep(0.02)
                        progress_bar.progress(i + 1)
                        
                        # Mise à jour du statut
                        if i < 30:
                            status_text.text("🔌 Connexion à l'API...")
                        elif i < 60:
                            status_text.text("📥 Téléchargement des séismes...")
                        elif i < 90:
                            status_text.text("💾 Écriture vers HDFS...")
                        else:
                            status_text.text("✅ Finalisation...")
                    
                    # Résultats
                    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                    st.success("✅ 850 séismes USGS collectés")
                    st.info(f"""
                    **Chemin HDFS:** `/hadoop-climate-risk/raw/usgs/earthquakes_{timestamp}.parquet`
                    
                    **Détails:**
                    - Format: Parquet (compressé)
                    - Taille: ~38 MB
                    - Période: Derniers 7 jours
                    - Magnitude min: 2.5
                    """)
                    
                    # Aperçu des données
                    with st.expander("📋 Aperçu des données ingérées"):
                        sample_df = pd.DataFrame({
                            'timestamp': pd.date_range('2024-01-01', periods=5, freq='H'),
                            'magnitude': [4.5, 3.2, 5.1, 2.8, 4.9],
                            'location': ['California', 'Alaska', 'Hawaii', 'Nevada', 'Texas'],
                            'depth_km': [10.2, 15.5, 8.7, 22.1, 12.4],
                            'latitude': [34.05, 36.17, 37.77, 40.71, 47.61],
                            'longitude': [-118.25, -120.72, -122.42, -74.01, -122.33]
                        })
                        st.dataframe(sample_df)
        
        # Upload manuel
        st.markdown("---")
        st.subheader("📤 Upload Manuel")
        
        uploaded_file = st.file_uploader(
            "Choisir un fichier à uploader vers HDFS",
            type=['csv', 'json', 'parquet', 'txt'],
            help="Formats supportés: CSV, JSON, Parquet"
        )
        
        if uploaded_file is not None:
            # Afficher les informations du fichier
            file_details = {
                "Nom": uploaded_file.name,
                "Type": uploaded_file.type,
                "Taille": f"{uploaded_file.size / 1024 / 1024:.2f} MB"
            }
            
            col1, col2 = st.columns(2)
            with col1:
                st.success(f"✅ {uploaded_file.name} prêt à être uploadé")
                for key, value in file_details.items():
                    st.write(f"**{key}:** {value}")
            
            with col2:
                destination = st.selectbox(
                    "Destination HDFS",
                    [
                        "/hadoop-climate-risk/raw/noaa/",
                        "/hadoop-climate-risk/raw/usgs/",
                        "/hadoop-climate-risk/alerts/",
                        "/hadoop-climate-risk/temp/"
                    ]
                )
                
                if st.button("📤 Upload vers HDFS", type="primary"):
                    with st.spinner(f"Upload vers {destination}..."):
                        time.sleep(2)
                        st.success(f"✅ Fichier uploadé vers {destination}{uploaded_file.name}")
                        
                        # Aperçu si c'est un CSV
                        if uploaded_file.name.endswith('.csv'):
                            df_upload = pd.read_csv(uploaded_file)
                            st.dataframe(df_upload.head(10), use_container_width=True)
    
    # ========== TAB 2 : TRAITEMENT ==========
    with tab2:
        st.subheader("Traitement Spark")
        
        # Liste des jobs
        jobs = {
            "🧹 Nettoyage ETL": {
                "desc": "Nettoyage des données brutes (valeurs manquantes, outliers)",
                "time": "3-5 min",
                "input": "/hadoop-climate-risk/raw/",
                "output": "/hadoop-climate-risk/silver/"
            },
            "📊 Agrégation quotidienne": {
                "desc": "Calcul des statistiques journalières",
                "time": "2-3 min",
                "input": "/hadoop-climate-risk/silver/",
                "output": "/hadoop-climate-risk/gold/aggregates/"
            },
            "🚨 Détection d'anomalies": {
                "desc": "Identification des valeurs aberrantes",
                "time": "4-6 min",
                "input": "/hadoop-climate-risk/silver/",
                "output": "/hadoop-climate-risk/gold/anomalies/"
            },
            "📈 Calcul des tendances": {
                "desc": "Analyse des tendances long terme",
                "time": "5-7 min",
                "input": "/hadoop-climate-risk/gold/",
                "output": "/hadoop-climate-risk/gold/trends/"
            }
        }
        
        # Sélection du job
        selected_job = st.selectbox(
            "Sélectionner un job Spark à exécuter",
            list(jobs.keys()),
            format_func=lambda x: f"{x} - {jobs[x]['desc'][:50]}..."
        )
        
        # Afficher les détails du job
        if selected_job:
            job_info = jobs[selected_job]
            st.info(f"""
            **Description:** {job_info['desc']}
            
            **Estimation temps:** {job_info['time']}
            **Input:** {job_info['input']}
            **Output:** {job_info['output']}
            """)
        
        # Bouton d'exécution
        if st.button(f"⚡ Exécuter {selected_job}", type="primary", use_container_width=True):
            with st.spinner(f"Exécution du job Spark: {selected_job}..."):
                # Barre de progression
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for percent in range(100):
                    time.sleep(0.03)
                    progress_bar.progress(percent + 1)
                    
                    # Messages de statut
                    if percent < 20:
                        status_text.text("🚀 Initialisation du job Spark...")
                    elif percent < 40:
                        status_text.text("📖 Lecture des données depuis HDFS...")
                    elif percent < 60:
                        status_text.text("⚙️ Traitement des données...")
                    elif percent < 80:
                        status_text.text("💾 Écriture des résultats...")
                    else:
                        status_text.text("✅ Finalisation...")
                
                # Résultats
                job_id = f"spark-{int(time.time())}"
                
                st.success(f"✅ Job {selected_job} terminé avec succès")
                
                # Détails d'exécution
                with st.expander("📋 Détails d'exécution", expanded=True):
                    st.code(f"""
                    Job ID: {job_id}
                    Status: SUCCEEDED
                    Duration: 2m 45s
                    Start Time: {datetime.now().strftime('%H:%M:%S')}
                    End Time: {(datetime.now() + timedelta(minutes=2, seconds=45)).strftime('%H:%M:%S')}
                    
                    Configuration Spark:
                    - Application: DataLake_ETL
                    - Master: spark://spark-master:7077
                    - Driver Memory: 2g
                    - Executor Cores: 2
                    - Partitions: 10
                    
                    Métriques:
                    - Records processed: 15,230
                    - Data read: 1.8 GB
                    - Data written: 450 MB
                    - Shuffle spill: 0 bytes
                    
                    Output:
                    - HDFS Path: {job_info['output']}
                    - Files: 12 part-*.parquet files
                    - Format: Parquet (snappy compression)
                    """)
                
                # Échantillon des résultats pour certains jobs
                if "Agrégation" in selected_job:
                    st.subheader("📊 Échantillon des Résultats")
                    
                    # Données simulées
                    if "quotidienne" in selected_job:
                        sample_results = pd.DataFrame({
                            'date': pd.date_range('2024-01-01', periods=7),
                            'avg_temperature': np.random.normal(15, 2, 7),
                            'max_temperature': np.random.normal(20, 3, 7),
                            'min_temperature': np.random.normal(10, 2, 7),
                            'total_precipitation': np.random.exponential(5, 7),
                            'earthquake_count': np.random.poisson(3, 7)
                        })
                    else:
                        sample_results = pd.DataFrame({
                            'metric': ['Température moyenne', 'Précipitation annuelle', 'Séismes majeurs'],
                            'value': [15.2, 1250.5, 24],
                            'trend': ['↑ +0.5°C', '↓ -10%', '→ stable'],
                            'period': ['2024', '2023-2024', 'Dernier mois']
                        })
                    
                    st.dataframe(sample_results, use_container_width=True)
    
    # ========== TAB 3 : EXPORT ==========
    with tab3:
        st.subheader("Export et Rapports")
        
        # Types de rapports
        report_types = {
            "📄 Rapport climatique complet": {
                "desc": "Analyse détaillée des tendances météorologiques",
                "size": "45 pages",
                "content": "Ce rapport analyse les tendances climatiques sur la dernière décennie..."
            },
            "📄 Analyse sismique régionale": {
                "desc": "Activité sismique par région avec visualisations",
                "size": "32 pages",
                "content": "Analyse de l'activité sismique dans les régions à risque..."
            },
            "📄 Étude de corrélation NOAA-USGS": {
                "desc": "Corrélations entre données météo et sismiques",
                "size": "28 pages",
                "content": "Étude statistique des corrélations entre variables climatiques et sismiques..."
            },
            "📊 Dashboard interactif (PDF)": {
                "desc": "Export PDF du dashboard actuel",
                "size": "15 pages",
                "content": "Snapshot interactif du dashboard DataLake avec toutes les visualisations..."
            },
            "💾 Dataset complet (CSV)": {
                "desc": "Export des données nettoyées au format CSV",
                "size": "~850 MB",
                "content": "Dataset complet prêt pour analyse externe..."
            }
        }
        
        # Sélection du rapport
        selected_report = st.selectbox(
            "Sélectionner un rapport à générer",
            list(report_types.keys())
        )
        
        if selected_report:
            report_info = report_types[selected_report]
            
            st.info(f"""
            **Description:** {report_info['desc']}
            **Taille estimée:** {report_info['size']}
            **Génération:** ~30 secondes
            """)
            
            # Bouton de génération
            if st.button(f"🔄 Générer {selected_report}", type="primary", use_container_width=True):
                with st.spinner(f"Génération du {selected_report}..."):
                    # Simulation de génération
                    progress_bar = st.progress(0)
                    
                    for i in range(100):
                        time.sleep(0.02)
                        progress_bar.progress(i + 1)
                    
                    st.success(f"✅ {selected_report} généré avec succès")
                    
                    # Bouton de téléchargement
                    st.download_button(
                        label=f"📥 Télécharger {selected_report}",
                        data=report_info['content'],
                        file_name=f"{selected_report.lower().replace('📄 ', '').replace('📊 ', '').replace('💾 ', '').replace(' ', '_')}.txt",
                        mime="text/plain",
                        help="Fichier de démonstration - en production, ce serait un PDF ou CSV"
                    )
                    
                    # Information supplémentaire
                    st.info(f"""
                    **Fichier généré:** `{selected_report.replace('📄 ', '').replace('📊 ', '').replace('💾 ', '').replace(' ', '_')}.pdf`
                    **Chemin HDFS:** `/hadoop-climate-risk/gold/reports/{datetime.now().strftime('%Y%m%d')}/`
                    **Timestamp:** {datetime.now().strftime('%H:%M:%S')}
                    """)

# ============================================
# PAGE 6 : ARCHITECTURE
# ============================================

elif menu == "🏗️ Architecture":
    
    st.header("🏗️ Architecture du DataLake")
    
    # ========== DIAGRAMME D'ARCHITECTURE ==========
    st.markdown("""
    ### 📊 Architecture Big Data Complète
    
    ```ascii
    ┌─────────────────────────────────────────────────────────────────────┐
    │                    COUCHE PRÉSENTATION (Streamlit)                  │
    │  ┌──────────────────────────────────────────────────────────────┐  │
    │  │                 Streamlit Dashboard v1.0                     │  │
    │  │  • Visualisations Plotly interactives                        │  │
    │  │  • Interface utilisateur multi-pages                         │  │
    │  │  • Contrôles d'administration                                │  │
    │  │  • Alertes temps réel                                        │  │
    │  └──────────────────────────────┬───────────────────────────────┘  │
    │                                 │ HTTP/WebSocket                    │
    ├─────────────────────────────────┼───────────────────────────────────┤
    │          COUCHE TRAITEMENT (Spark + Python)                        │
    │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐                │
    │  │   Spark     │  │   Kafka     │  │   Python    │                │
    │  │  Cluster    │  │  Streaming  │  │  Ingestion  │                │
    │  │  • ETL Jobs │  │  • Topics   │  │  • APIs     │                │
    │  │  • ML/Stats │  │  • Alerts   │  │  • Batch    │                │
    │  │  • Analytics│  │  • Logs     │  │  • Scripts  │                │
    │  └──────┬──────┘  └──────┬──────┘  └──────┬──────┘                │
    │         │                │                │                        │
    ├─────────┼────────────────┼────────────────┼────────────────────────┤
    │                    COUCHE STOCKAGE (HDFS)                          │
    │         ┌──────────────────────────────────────────────┐           │
    │         │              HDFS DataLake                   │           │
    │         │  ┌─────────┬─────────┬─────────┬─────────┐  │           │
    │         │  │  RAW    │  SILVER │   GOLD  │ ALERTS  │  │           │
    │         │  │  Layer  │  Layer  │  Layer  │  Layer  │  │           │
    │         │  │  • NOAA │  • ETL  │ • Aggr. │ • Kafka │  │           │
    │         │  │  • USGS │  • Clean│ • Stats │ • Stream│  │           │
    │         │  └─────────┴─────────┴─────────┴─────────┘  │           │
    │         └──────────────────────────────────────────────┘           │
    ├─────────────────────────────────────────────────────────────────────┤
    │                    COUCHE SOURCES (APIs)                           │
    │  ┌───────────────────┐            ┌───────────────────┐           │
    │  │      NOAA API     │            │      USGS API     │           │
    │  │  • weather.gov    │            │  earthquake.usgs  │           │
    │  │  • data.noaa.gov  │            │  • Realtime       │           │
    │  │  • CSV/JSON       │            │  • Historical     │           │
    │  │  • Realtime       │            │  • GeoJSON        │           │
    │  └───────────────────┘            └───────────────────┘           │
    └─────────────────────────────────────────────────────────────────────┘
    ```
    """)
    
    # ========== STACK TECHNOLOGIQUE ==========
    st.markdown("### 🛠️ Stack Technologique")
    
    tech_stack = pd.DataFrame({
        "Couche": ["Stockage", "Traitement", "Streaming", "Visualisation", "Orchestration", "Sources"],
        "Technologies": [
            "HDFS (Hadoop Distributed File System)",
            "Apache Spark, Python (Pandas, PySpark)",
            "Apache Kafka, Spark Streaming",
            "Streamlit, Plotly, Altair",
            "Docker, Docker Compose, Kubernetes",
            "NOAA API, USGS API, OpenData"
        ],
        "Rôle": [
            "Stockage distribué des données brutes et transformées",
            "ETL, analyse, machine learning, agrégations",
            "Traitement temps réel, alertes, monitoring",
            "Dashboard interactif, visualisations, rapports",
            "Conteneurisation, déploiement, scaling",
            "Sources de données externes en temps réel"
        ]
    })
    
    st.dataframe(tech_stack, use_container_width=True, hide_index=True)
    
    # ========== FLUX DE DONNÉES ==========
    st.markdown("### 📈 Flux de Données")
    
    st.info("""
    **1. 📥 Ingestion (Batch + Streaming)**
    ```
    Sources externes → Python Scripts → HDFS (Raw) + Kafka
    ```
    
    **2. ⚙️ Transformation (ETL)**
    ```
    HDFS (Raw) → Spark Jobs → HDFS (Silver)
    • Nettoyage des données
    • Normalisation
    • Enrichissement
    ```
    
    **3. 📊 Analyse & Agrégation**
    ```
    HDFS (Silver) → Spark Analytics → HDFS (Gold)
    • Agrégations temporelles
    • Calculs statistiques
    • Machine Learning
    • Détection d'anomalies
    ```
    
    **4. 🎯 Visualisation & Insights**
    ```
    HDFS (Gold) → Streamlit → Dashboard
    • Visualisations interactives
    • Rapports automatiques
    • Alertes temps réel
    • Export de données
    ```
    
    **5. 🔄 Streaming & Monitoring**
    ```
    Kafka Topics → Spark Streaming → Alerts → HDFS/Streamlit
    • Monitoring continu
    • Alertes en temps réel
    • Logs et métriques
    ```
    """)
    
    # ========== AVANTAGES ==========
    st.markdown("### 🎯 Avantages de l'Architecture")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
        **🚀 Scalabilité**
        - HDFS: Stockage distribué illimité
        - Spark: Traitement parallèle
        - Docker: Déploiement flexible
        """)
    
    with col2:
        st.markdown("""
        **🔄 Temps Réel**
        - Kafka: Streaming de données
        - Alertes instantanées
        - Monitoring continu
        """)
    
    with col3:
        st.markdown("""
        **🔧 Maintenance**
        - Architecture modulaire
        - Code versionné (Git)
        - Documentation complète
        """)
    
    # ========== MÉTRIQUES DE PERFORMANCE ==========
    st.markdown("### 📊 Métriques de Performance")
    
    metrics = pd.DataFrame({
        "Métrique": ["Latence ingestion", "Temps traitement", "Disponibilité", "Volume données", "Coût stockage"],
        "Valeur": ["< 5 min", "2-5 min/job", "99.9%", "~2.4 GB", "~$15/mois"],
        "Objectif": ["Temps réel", "Rapidité", "Haute dispo", "Scalable", "Économique"]
    })
    
    st.dataframe(metrics, use_container_width=True, hide_index=True)

# ============================================
# FOOTER
# ============================================

st.sidebar.markdown("---")
st.sidebar.markdown("**DataLake Climat & Risques Naturels**")
st.sidebar.markdown("*Projet Big Data - Architecture*")

# Informations de version
with st.sidebar.expander("ℹ️ Informations"):
    st.write(f"**Version:** 1.0.0")
    st.write(f"**Dernière mise à jour:** {datetime.now().strftime('%d/%m/%Y')}")
    st.write(f"**Environnement:** Production")
    st.write("**Équipe:** Data Engineering Team")
    st.write("**Contact:** datalake@climate-risks.com")

# Note de bas de page
st.sidebar.caption("""
⚠️ **Note:** Cette application est une démonstration.
Les données sont simulées pour illustrer les capacités du DataLake.
""")
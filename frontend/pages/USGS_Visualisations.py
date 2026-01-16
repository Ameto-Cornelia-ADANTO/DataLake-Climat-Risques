import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

st.set_page_config(page_title="USGS Visualisations", page_icon="🌋", layout="wide")

st.title("🌋 Visualisations USGS - Données Sismiques")

st.info("ℹ️ Cette page montre les visualisations détaillées des données USGS (United States Geological Survey)")

# Charger les données USGS
@st.cache_data
def load_usgs_data():
    # Générer des données sismiques simulées
    n_events = 200
    regions = ['California', 'Alaska', 'Hawaii', 'Nevada', 'Washington', 'Oregon', 'Utah', 'Montana']
    
    # Coordonnées approximatives par région
    region_coords = {
        'California': (36.7783, -119.4179),
        'Alaska': (64.2008, -149.4937),
        'Hawaii': (19.8968, -155.5828),
        'Nevada': (38.8026, -116.4194),
        'Washington': (47.7511, -120.7401),
        'Oregon': (43.8041, -120.5542),
        'Utah': (39.3210, -111.0937),
        'Montana': (46.8797, -110.3626)
    }
    
    data = []
    for i in range(n_events):
        region = np.random.choice(regions)
        lat, lon = region_coords[region]
        
        # Générer des coordonnées aléatoires autour du centre de la région
        event_lat = lat + np.random.uniform(-2, 2)
        event_lon = lon + np.random.uniform(-2, 2)
        
        # Générer une magnitude (distribution exponentielle inversée)
        magnitude = np.random.exponential(scale=1.5) + 2.0
        if magnitude > 9.0:
            magnitude = 9.0
        
        # Générer une profondeur
        depth = np.random.exponential(scale=20) + 1
        
        # Générer un timestamp aléatoire dans les 30 derniers jours
        days_ago = np.random.uniform(0, 30)
        timestamp = datetime.now() - timedelta(days=days_ago)
        
        data.append({
            'Timestamp': timestamp,
            'Magnitude': round(magnitude, 1),
            'Depth_km': round(depth, 1),
            'Region': region,
            'Latitude': round(event_lat, 4),
            'Longitude': round(event_lon, 4),
            'Day_of_Year': timestamp.timetuple().tm_yday
        })
    
    df = pd.DataFrame(data)
    
    # Catégoriser par intensité
    df['Intensity'] = pd.cut(df['Magnitude'], 
                            bins=[0, 3, 4, 5, 6, 7, 10],
                            labels=['Très Faible', 'Faible', 'Léger', 'Modéré', 'Fort', 'Majeur'])
    
    return df

df_usgs = load_usgs_data()

# Afficher les métriques principales
st.subheader("📈 Métriques Globales")

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("🌋 Séismes totaux", len(df_usgs))
with col2:
    max_mag = df_usgs['Magnitude'].max()
    st.metric("📈 Magnitude max", f"{max_mag:.1f}")
with col3:
    avg_depth = df_usgs['Depth_km'].mean()
    st.metric("⬇️ Profondeur moyenne", f"{avg_depth:.1f} km")
with col4:
    st.metric("🗺️ Régions", df_usgs['Region'].nunique())

# Onglets pour différentes visualisations
tab1, tab2, tab3, tab4 = st.tabs(["📊 Analyses", "🗺️ Carte", "📋 Données", "⏱️ Évolution Temporelle"])

with tab1:
    # Filtres
    st.subheader("🔍 Filtres")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        min_magnitude = st.slider(
            "Magnitude minimale",
            float(df_usgs['Magnitude'].min()),
            float(df_usgs['Magnitude'].max()),
            2.5
        )
    
    with col2:
        selected_regions = st.multiselect(
            "Sélectionner les régions",
            df_usgs['Region'].unique(),
            default=df_usgs['Region'].unique()[:3]
        )
    
    with col3:
        intensity_filter = st.multiselect(
            "Intensité",
            df_usgs['Intensity'].unique(),
            default=df_usgs['Intensity'].unique()
        )
    
    # Filtrer les données
    filtered_df = df_usgs[
        (df_usgs['Magnitude'] >= min_magnitude) &
        (df_usgs['Region'].isin(selected_regions)) &
        (df_usgs['Intensity'].isin(intensity_filter))
    ]
    
    if filtered_df.empty:
        st.warning("⚠️ Aucun séisme ne correspond aux filtres sélectionnés")
    else:
        col1, col2 = st.columns(2)
        
        with col1:
            # Histogramme des magnitudes
            st.markdown("#### 📊 Distribution des Magnitudes")
            fig_hist = px.histogram(
                filtered_df,
                x='Magnitude',
                nbins=30,
                color='Region',
                title='Distribution des Magnitudes',
                marginal='box'
            )
            fig_hist.update_layout(height=500)
            st.plotly_chart(fig_hist, use_container_width=True)
        
        with col2:
            # Box plot par région
            st.markdown("#### 📦 Magnitudes par Région")
            fig_box = px.box(
                filtered_df,
                x='Region',
                y='Magnitude',
                color='Region',
                title='Comparaison Régionale',
                points='all'
            )
            fig_box.update_layout(height=500)
            st.plotly_chart(fig_box, use_container_width=True)
        
        # Scatter plot Magnitude vs Profondeur
        st.markdown("#### 📍 Relation Magnitude-Profondeur")
        fig_scatter = px.scatter(
            filtered_df,
            x='Magnitude',
            y='Depth_km',
            color='Region',
            size='Magnitude',
            hover_data=['Timestamp', 'Intensity'],
            title='Magnitude vs Profondeur'
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
        
        # Diagramme de répartition
        st.markdown("#### 🎯 Répartition par Intensité")
        intensity_counts = filtered_df['Intensity'].value_counts().reset_index()
        intensity_counts.columns = ['Intensité', 'Nombre']
        
        fig_pie = px.pie(
            intensity_counts,
            values='Nombre',
            names='Intensité',
            title='Répartition par Niveau d\'Intensité',
            color='Intensité',
            color_discrete_map={
                'Très Faible': '#00FF00',
                'Faible': '#7CFC00',
                'Léger': '#FFFF00',
                'Modéré': '#FFA500',
                'Fort': '#FF4500',
                'Majeur': '#FF0000'
            }
        )
        st.plotly_chart(fig_pie, use_container_width=True)

with tab2:
    st.subheader("🗺️ Carte Interactive des Séismes")
    
    # Légende des couleurs
    st.markdown("""
    ### 🎨 Légende des Intensités:
    - 🟢 **Très Faible** (0-3.0)
    - 🟡 **Faible** (3.0-4.0)
    - 🟠 **Léger** (4.0-5.0)
    - 🔴 **Modéré** (5.0-6.0)
    - 🟥 **Fort** (6.0-7.0)
    - ⚫ **Majeur** (7.0+)
    """)
    
    # Filtres pour la carte
    col1, col2 = st.columns(2)
    with col1:
        map_mag_min = st.slider(
            "Filtre magnitude (carte)",
            float(df_usgs['Magnitude'].min()),
            float(df_usgs['Magnitude'].max()),
            3.0
        )
    with col2:
        days_back = st.slider("Période (jours)", 1, 365, 30)
    
    # Filtrer les données pour la carte
    date_threshold = datetime.now() - timedelta(days=days_back)
    map_data = df_usgs[
        (df_usgs['Magnitude'] >= map_mag_min) &
        (df_usgs['Timestamp'] >= date_threshold)
    ].copy()
    
    if map_data.empty:
        st.warning("⚠️ Aucun séisme récent ne correspond aux filtres")
    else:
        # CORRECTION : Préparer les données pour la carte de manière simple
        # Créer une copie simple des données nécessaires
        map_data_simple = map_data[['Latitude', 'Longitude', 'Magnitude', 'Intensity']].copy()
        
        # Ajouter une colonne de taille proportionnelle à la magnitude
        map_data_simple['size'] = map_data_simple['Magnitude'].apply(lambda x: min(50, x * 5))
        
        # Mapper les intensités aux couleurs
        color_map_simple = {
            'Très Faible': '#00FF00',
            'Faible': '#7CFC00',
            'Léger': '#FFFF00',
            'Modéré': '#FFA500',
            'Fort': '#FF4500',
            'Majeur': '#FF0000'
        }
        
        # S'assurer que toutes les intensités ont une couleur
        map_data_simple['color'] = map_data_simple['Intensity'].apply(
            lambda x: color_map_simple.get(str(x), '#808080')
        )
        
        # Afficher la carte SIMPLIFIÉE (sans paramètres problématiques)
        try:
            st.map(map_data_simple[['Latitude', 'Longitude']].dropna())
        except Exception as e:
            st.error(f"Erreur avec la carte : {str(e)[:100]}")
            # Solution de secours
            st.write("📊 Données sur la carte :")
            st.dataframe(map_data_simple.head(10))
        
        # Statistiques sur la carte
        st.subheader("📊 Statistiques de la Carte")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Séismes affichés", len(map_data))
        with col2:
            st.metric("Magnitude moyenne", f"{map_data['Magnitude'].mean():.1f}")
        with col3:
            st.metric("Dernier séisme", map_data['Timestamp'].max().strftime('%Y-%m-%d'))
        with col4:
            st.metric("Régions concernées", map_data['Region'].nunique())
        
        # Carte de densité
        st.subheader("🗺️ Carte de Densité")
        
        # Créer une heatmap simplifiée
        fig_density = px.density_mapbox(
            map_data,
            lat='Latitude',
            lon='Longitude',
            z='Magnitude',
            radius=15,
            center=dict(lat=40, lon=-100),
            zoom=3,
            mapbox_style="carto-positron",
            title='Densité des Séismes (Magnitude)',
            height=500
        )
        st.plotly_chart(fig_density, use_container_width=True)
        
        # Visualisation 3D
        st.subheader("🛸 Visualisation 3D")
        
        fig_3d = px.scatter_3d(
            map_data,
            x='Longitude',
            y='Latitude',
            z='Depth_km',
            color='Magnitude',
            size='Magnitude',
            hover_data=['Region', 'Timestamp', 'Intensity'],
            title='Localisation 3D des Séismes',
            height=600
        )
        st.plotly_chart(fig_3d, use_container_width=True)

with tab3:
    st.subheader("📋 Données Brutes")
    
    # Options d'affichage
    col1, col2, col3 = st.columns(3)
    with col1:
        rows_to_show = st.selectbox("Lignes à afficher", [10, 25, 50, 100, 200])
    with col2:
        sort_by = st.selectbox("Trier par", ['Timestamp', 'Magnitude', 'Depth_km', 'Region'])
    with col3:
        sort_order = st.radio("Ordre", ['Décroissant', 'Croissant'], horizontal=True)
    
    # Trier les données
    ascending = sort_order == 'Croissant'
    sorted_data = df_usgs.sort_values(sort_by, ascending=ascending)
    
    # Afficher les données
    st.dataframe(
        sorted_data.head(rows_to_show),
        use_container_width=True,
        height=400
    )
    
    # Statistiques par région
    st.subheader("📊 Statistiques par Région")
    
    region_stats = df_usgs.groupby('Region').agg({
        'Magnitude': ['count', 'mean', 'max', 'min'],
        'Depth_km': ['mean'],
        'Timestamp': ['min', 'max']
    }).round(2)
    
    # Aplatir les colonnes MultiIndex
    region_stats.columns = ['_'.join(col).strip() for col in region_stats.columns.values]
    region_stats = region_stats.reset_index()
    
    st.dataframe(region_stats, use_container_width=True)
    
    # Bouton de téléchargement
    csv_data = df_usgs.to_csv(index=False)
    st.download_button(
        label="📥 Télécharger les données (CSV)",
        data=csv_data,
        file_name=f"usgs_earthquakes_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        mime="text/csv"
    )

with tab4:
    st.subheader("⏱️ Évolution Temporelle")
    
    # Agrégation par jour
    df_usgs['Date'] = df_usgs['Timestamp'].dt.date
    daily_stats = df_usgs.groupby('Date').agg({
        'Magnitude': ['count', 'mean', 'max'],
        'Depth_km': ['mean']
    }).round(2)
    
    # Aplatir les colonnes MultiIndex
    daily_stats.columns = ['_'.join(col).strip() for col in daily_stats.columns.values]
    daily_stats = daily_stats.reset_index()
    
    # Graphique d'évolution
    fig_evolution = go.Figure()
    
    # Ajouter la courbe du nombre de séismes
    fig_evolution.add_trace(go.Scatter(
        x=daily_stats['Date'],
        y=daily_stats['Magnitude_count'],
        mode='lines+markers',
        name='Nombre de séismes',
        line=dict(color='blue', width=2),
        yaxis='y'
    ))
    
    # Ajouter la courbe de magnitude moyenne
    fig_evolution.add_trace(go.Scatter(
        x=daily_stats['Date'],
        y=daily_stats['Magnitude_mean'],
        mode='lines+markers',
        name='Magnitude moyenne',
        line=dict(color='red', width=2),
        yaxis='y2'
    ))
    
    fig_evolution.update_layout(
        title='Évolution Journalière des Séismes',
        xaxis_title='Date',
        yaxis=dict(
            title='Nombre de séismes',
            title_font=dict(color='blue'),
            tickfont=dict(color='blue')
        ),
        yaxis2=dict(
            title='Magnitude moyenne',
            title_font=dict(color='red'),
            tickfont=dict(color='red'),
            overlaying='y',
            side='right'
        ),
        height=500,
        hovermode='x unified'
    )
    
    st.plotly_chart(fig_evolution, use_container_width=True)
    
    # Analyse des patterns
    st.subheader("🔍 Analyse des Patterns")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Analyse par jour de la semaine
        df_usgs['Weekday'] = df_usgs['Timestamp'].dt.day_name()
        weekday_counts = df_usgs['Weekday'].value_counts().reindex([
            'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'
        ])
        
        fig_weekday = px.bar(
            x=weekday_counts.index,
            y=weekday_counts.values,
            title='Distribution par Jour de la Semaine',
            labels={'x': 'Jour', 'y': 'Nombre de séismes'},
            color=weekday_counts.values,
            color_continuous_scale='Reds'
        )
        st.plotly_chart(fig_weekday, use_container_width=True)
    
    with col2:
        # Analyse par heure
        df_usgs['Hour'] = df_usgs['Timestamp'].dt.hour
        hourly_counts = df_usgs['Hour'].value_counts().sort_index()
        
        fig_hour = px.bar(
            x=hourly_counts.index,
            y=hourly_counts.values,
            title='Distribution par Heure',
            labels={'x': 'Heure', 'y': 'Nombre de séismes'},
            color=hourly_counts.values,
            color_continuous_scale='Oranges'
        )
        st.plotly_chart(fig_hour, use_container_width=True)
    
    # Corrélations
    st.subheader("📊 Matrice de Corrélation")
    
    # Utiliser des noms de colonnes simples
    corr_data = df_usgs[['Magnitude', 'Depth_km', 'Day_of_Year', 'Latitude', 'Longitude']].copy()
    
    # Renommer pour plus de clarté
    corr_data = corr_data.rename(columns={
        'Depth_km': 'Profondeur_km',
        'Day_of_Year': 'Jour_Annee'
    })
    
    corr_matrix = corr_data.corr()
    
    fig_corr = px.imshow(
        corr_matrix,
        text_auto=True,
        aspect='auto',
        color_continuous_scale='RdBu',
        title='Corrélations entre Variables'
    )
    st.plotly_chart(fig_corr, use_container_width=True)

# Informations sur les données
with st.expander("ℹ️ À propos des données USGS", expanded=False):
    st.markdown("""
    ## 🌐 Source des données: USGS (United States Geological Survey)
    
    ### 📊 Échelle de Richter:
    - **0-3.0:** Très faible - Généralement non ressenti
    - **3.0-4.0:** Faible - Ressenti par quelques personnes
    - **4.0-5.0:** Léger - Dommages mineurs possibles
    - **5.0-6.0:** Modéré - Dommages significatifs
    - **6.0-7.0:** Fort - Dommages majeurs
    - **7.0+:** Majeur - Catastrophe régionale
    
    ### 🎯 Types de données collectées:
    - **Magnitude** - Énergie libérée
    - **Profondeur** (km) - Foyer sismique
    - **Localisation** - Coordonnées GPS
    - **Timestamp** - Date et heure précise
    - **Région** - Zone géographique
    
    ### 🗺️ Zones d'étude:
    - **États-Unis continentaux**
    - **Alaska** - Zone très active
    - **Hawaii** - Activité volcanique
    - **Ouest des USA** - Faille de San Andreas
    
    ### 🔧 Qualité des données:
    - **Validées en temps réel**
    - **Mises à jour continues**
    - **Format standardisé**
    - **Historique complet**
    
    ### 🎯 Utilisations principales:
    - Évaluation des risques sismiques
    - Recherche géologique
    - Planification d'urgence
    - Construction parasismique
    - Alerte précoce
    
    ### ⚠️ Limitations:
    - Données simulées pour cette démonstration
    - En production: API USGS temps réel
    - Délai de traitement: quelques minutes
    - Couverture: mondiale mais focus USA
    """)
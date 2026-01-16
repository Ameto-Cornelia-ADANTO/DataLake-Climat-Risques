import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from utils.hdfs_client import HDFSClient
import json
import random

st.set_page_config(page_title="Alertes Temps Réel", layout="wide")
st.title("🚨 Alertes Temps Réel")
st.markdown("### Monitoring des événements climatiques et sismiques en direct")

# Initialisation clients
hdfs_client = HDFSClient()

# Configuration
if "auto_refresh" not in st.session_state:
    st.session_state.auto_refresh = False
if "alert_history" not in st.session_state:
    st.session_state.alert_history = []

# Sidebar pour les paramètres
with st.sidebar:
    st.header("⚙️ Paramètres")
    
    # Type d'alertes à afficher
    alert_types = st.multiselect(
        "Types d'alertes",
        ["Séismes", "Tempêtes", "Inondations", "Vagues de chaleur", "Incendies"],
        default=["Séismes", "Tempêtes"]
    )
    
    # Niveau de sévérité
    severity_level = st.slider(
        "Niveau de sévérité minimum",
        min_value=1, max_value=5, value=2,
        help="1=Faible, 3=Modéré, 5=Extrême"
    )
    
    # Régions
    regions = st.multiselect(
        "Régions",
        ["Amérique du Nord", "Europe", "Asie", "Amérique du Sud", "Afrique", "Océanie"],
        default=["Amérique du Nord", "Europe"]
    )
    
    # Auto-refresh
    auto_refresh = st.checkbox("🔄 Auto-refresh", value=st.session_state.auto_refresh)
    refresh_interval = st.slider("Intervalle (secondes)", 5, 60, 10, disabled=not auto_refresh)
    
    if auto_refresh != st.session_state.auto_refresh:
        st.session_state.auto_refresh = auto_refresh
        st.rerun()

# Fonctions de simulation
def simulate_real_time_alerts():
    """Simule la réception d'alertes temps réel"""
    alert_types_sim = ["Séisme", "Tempête", "Inondation", "Vague de chaleur", "Incendie"]
    regions_sim = ["Californie", "Alaska", "Floride", "Texas", "New York", "Québec", "Paris", "Tokyo"]
    
    alert = {
        "id": f"alert_{int(time.time())}_{random.randint(1000, 9999)}",
        "type": random.choice(alert_types),
        "severity": random.randint(1, 5),
        "region": random.choice(regions_sim),
        "latitude": round(random.uniform(30, 50), 4),
        "longitude": round(random.uniform(-130, -60), 4),
        "magnitude": round(random.uniform(2.0, 8.0), 1) if random.random() > 0.5 else None,
        "wind_speed": round(random.uniform(50, 200), 1) if random.random() > 0.5 else None,
        "temperature": round(random.uniform(35, 50), 1) if random.random() > 0.5 else None,
        "description": f"Événement {random.choice(['majeur', 'modéré', 'mineur'])} détecté",
        "timestamp": datetime.now().isoformat(),
        "source": random.choice(["USGS", "NOAA", "NASA", "Météo France"])
    }
    
    # Ajouter à l'historique
    st.session_state.alert_history.append(alert)
    
    # Garder seulement les 100 dernières alertes
    if len(st.session_state.alert_history) > 100:
        st.session_state.alert_history = st.session_state.alert_history[-100:]
    
    return alert

def load_alerts_from_hdfs():
    """Charge les alertes depuis HDFS"""
    alerts = []
    
    if hdfs_client.connected:
        try:
            # Lire les fichiers d'alertes
            alert_files = hdfs_client.list_files("/hadoop-climate-risk/alerts/")
            
            for file in alert_files[-5:]:  # 5 fichiers les plus récents
                if file.endswith(".parquet"):
                    df = hdfs_client.read_parquet_head(file, 20)
                    if not df.empty and "Error" not in df.columns:
                        for _, row in df.iterrows():
                            alert = {
                                "type": row.get("alert_type", "Inconnu"),
                                "severity": row.get("severity", 1),
                                "region": row.get("region", "Inconnu"),
                                "latitude": row.get("latitude", 0.0),
                                "longitude": row.get("longitude", 0.0),
                                "timestamp": row.get("timestamp", datetime.now().isoformat()),
                                "description": row.get("description", "Alerte non spécifiée"),
                                "source": "HDFS"
                            }
                            alerts.append(alert)
        except:
            pass
    
    return alerts

# Layout principal
col1, col2 = st.columns([2, 1])

with col1:
    # Carte des alertes en temps réel
    st.subheader("📍 Carte des Alertes Actives")
    
    # Préparer les données pour la carte
    all_alerts = load_alerts_from_hdfs()
    
    # Ajouter des alertes simulées
    if st.button("🔄 Simuler nouvelle alerte"):
        new_alert = simulate_real_time_alerts()
        all_alerts.append(new_alert)
        st.success(f"✅ Nouvelle alerte simulée: {new_alert['type']} - {new_alert['region']}")
    
    if all_alerts:
        # Filtrer par sévérité
        filtered_alerts = [a for a in all_alerts if a.get("severity", 1) >= severity_level]
        
        # Filtrer par type
        if alert_types:
            filtered_alerts = [a for a in filtered_alerts 
                             if any(t in a.get("type", "") for t in alert_types)]
        
        # Filtrer par région
        if regions:
            filtered_alerts = [a for a in filtered_alerts 
                             if any(r in a.get("region", "") for r in regions)]
        
        if filtered_alerts:
            # Créer DataFrame pour Plotly
            df_alerts = pd.DataFrame(filtered_alerts)
            
            # Personnaliser la taille des marqueurs par sévérité
            df_alerts["size"] = df_alerts["severity"] * 10
            
            # Personnaliser les couleurs par type
            color_discrete_map = {
                "Séisme": "red",
                "Tempête": "blue", 
                "Inondation": "cyan",
                "Vague de chaleur": "orange",
                "Incendie": "darkred"
            }
            
            fig = px.scatter_mapbox(
                df_alerts,
                lat="latitude",
                lon="longitude",
                color="type",
                size="size",
                hover_name="description",
                hover_data=["region", "severity", "timestamp", "source"],
                zoom=2,
                height=500,
                color_discrete_map=color_discrete_map
            )
            
            fig.update_layout(
                mapbox_style="open-street-map",
                margin={"r":0, "t":0, "l":0, "b":0}
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Statistiques
            col_stats1, col_stats2, col_stats3 = st.columns(3)
            with col_stats1:
                st.metric("Alertes actives", len(filtered_alerts))
            with col_stats2:
                avg_severity = df_alerts["severity"].mean() if not df_alerts.empty else 0
                st.metric("Sévérité moyenne", f"{avg_severity:.1f}/5")
            with col_stats3:
                if not df_alerts.empty:
                    most_common = df_alerts["type"].mode().iloc[0] if not df_alerts["type"].mode().empty else "Aucun"
                    st.metric("Type dominant", most_common)
        else:
            st.info("ℹ️ Aucune alerte ne correspond aux filtres actuels")
    else:
        st.warning("⚠️ Aucune alerte disponible. Simulez une alerte ou vérifiez HDFS.")

with col2:
    # Liste des dernières alertes
    st.subheader("📋 Dernières Alertes")
    
    # Trier par timestamp
    if st.session_state.alert_history:
        recent_alerts = sorted(
            st.session_state.alert_history,
            key=lambda x: x.get("timestamp", ""),
            reverse=True
        )[:10]  # 10 plus récentes
        
        for alert in recent_alerts:
            # Déterminer l'icône et la couleur par type
            alert_type = alert.get("type", "Inconnu")
            severity = alert.get("severity", 1)
            
            icons = {
                "Séisme": "🌋",
                "Tempête": "🌪️",
                "Inondation": "🌊",
                "Vague de chaleur": "🔥",
                "Incendie": "🚒"
            }
            
            icon = icons.get(alert_type, "⚠️")
            
            # Couleur par sévérité
            severity_colors = {
                1: "🟢", 2: "🟡", 3: "🟠", 4: "🔴", 5: "💀"
            }
            
            severity_icon = severity_colors.get(severity, "⚪")
            
            # Afficher l'alerte
            with st.expander(f"{icon} {severity_icon} {alert_type} - {alert.get('region', 'Inconnu')}"):
                st.write(f"**Description:** {alert.get('description', 'N/A')}")
                st.write(f"**Sévérité:** {severity}/5")
                st.write(f"**Source:** {alert.get('source', 'Inconnu')}")
                
                if alert.get("magnitude"):
                    st.write(f"**Magnitude:** {alert['magnitude']}")
                if alert.get("wind_speed"):
                    st.write(f"**Vitesse vent:** {alert['wind_speed']} km/h")
                if alert.get("temperature"):
                    st.write(f"**Température:** {alert['temperature']}°C")
                
                st.caption(f"⏰ {alert.get('timestamp', '')}")
    else:
        st.info("Aucune alerte récente. Cliquez sur 'Simuler nouvelle alerte'")

# Section analyse temporelle
st.markdown("---")
st.subheader("📈 Analyse Temporelle des Alertes")

if st.session_state.alert_history:
    df_history = pd.DataFrame(st.session_state.alert_history)
    
    # Convertir les timestamps
    df_history["datetime"] = pd.to_datetime(df_history["timestamp"])
    df_history["hour"] = df_history["datetime"].dt.hour
    df_history["date"] = df_history["datetime"].dt.date
    
    # Graphique 1: Alertes par heure
    col_hist1, col_hist2 = st.columns(2)
    
    with col_hist1:
        hourly_counts = df_history.groupby("hour").size().reset_index(name="count")
        fig_hourly = px.bar(
            hourly_counts,
            x="hour",
            y="count",
            title="Alertes par heure de la journée",
            labels={"hour": "Heure", "count": "Nombre d'alertes"}
        )
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    with col_hist2:
        # Graphique 2: Répartition par type
        type_counts = df_history["type"].value_counts().reset_index()
        type_counts.columns = ["type", "count"]
        
        fig_types = px.pie(
            type_counts,
            values="count",
            names="type",
            title="Répartition par type d'alerte",
            hole=0.3
        )
        st.plotly_chart(fig_types, use_container_width=True)
    
    # Graphique 3: Évolution de la sévérité
    if "severity" in df_history.columns:
        df_history["severity_numeric"] = pd.to_numeric(df_history["severity"], errors='coerce')
        severity_over_time = df_history.groupby("date")["severity_numeric"].mean().reset_index()
        
        fig_severity = px.line(
            severity_over_time,
            x="date",
            y="severity_numeric",
            title="Évolution de la sévérité moyenne",
            labels={"severity_numeric": "Sévérité moyenne", "date": "Date"}
        )
        st.plotly_chart(fig_severity, use_container_width=True)

# Section configuration Kafka (si connecté)
st.markdown("---")
st.subheader("🔧 Configuration Streaming")

tab_kafka, tab_hdfs = st.tabs(["Kafka", "HDFS"])

with tab_kafka:
    st.markdown("""
    **Configuration Kafka pour le streaming:**
    
    - **Topic:** `climate-alerts`
    - **Broker:** `kafka:9092`
    - **Format:** JSON
    - **Consommateurs:** Spark Streaming, Python Consumer
    """)
    
    if st.button("📡 Tester connexion Kafka"):
        try:
            from kafka import KafkaConsumer, KafkaProducer
            producer = KafkaProducer(bootstrap_servers='kafka:9092')
            st.success("✅ Connexion Kafka établie")
            
            # Envoyer un message test
            test_message = {
                "test": True,
                "timestamp": datetime.now().isoformat(),
                "message": "Test depuis Streamlit"
            }
            producer.send('climate-alerts', json.dumps(test_message).encode('utf-8'))
            st.info("📤 Message test envoyé au topic 'climate-alerts'")
            
        except Exception as e:
            st.error(f"❌ Erreur Kafka: {e}")

with tab_hdfs:
    st.markdown("**Statut HDFS:**")
    
    if hdfs_client.connected:
        st.success("✅ HDFS connecté")
        
        # Afficher le nombre d'alertes stockées
        try:
            alert_files = hdfs_client.list_files("/hadoop-climate-risk/alerts/")
            parquet_files = [f for f in alert_files if f.endswith(".parquet")]
            
            st.metric("Fichiers d'alertes", len(parquet_files))
            
            if parquet_files:
                # Afficher les derniers fichiers
                with st.expander("Derniers fichiers d'alertes"):
                    for file in parquet_files[-5:]:
                        st.write(f"📄 {file.split('/')[-1]}")
                        
                        # Bouton pour prévisualiser
                        if st.button(f"Aperçu {file.split('/')[-1]}", key=f"preview_{file}"):
                            df_preview = hdfs_client.read_parquet_head(file, 5)
                            st.dataframe(df_preview)
        except:
            st.warning("Impossible de lire le dossier des alertes")
    else:
        st.error("❌ HDFS non connecté")

# Auto-refresh
if st.session_state.auto_refresh:
    time.sleep(refresh_interval)
    st.rerun()

# Footer
st.markdown("---")
st.caption("⚠️ **Note:** Les alertes sont simulées. Pour des données réelles, configurez les sources USGS/NOAA en temps réel.")
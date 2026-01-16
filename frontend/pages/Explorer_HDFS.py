import streamlit as st
from utils.hdfs_client import HDFSClient

st.title("📁 Explorer HDFS")
st.markdown("### Naviguez dans l'architecture du DataLake")

client = HDFSClient()

if not client.connected:
    st.error("""
    ❌ **HDFS non connecté**
    - Vérifiez que le service HDFS est démarré
    - Vérifiez la configuration dans `hdfs_client.py`
    """)
    st.stop()

# Navigation
path = st.text_input("Chemin HDFS", "/hadoop-climate-risk")

if st.button("📂 Lister le contenu") or path:
    with st.spinner(f"Chargement de {path}..."):
        try:
            items = client.list_files(path)
            
            if items:
                # Séparer fichiers et dossiers
                folders = []
                files = []
                
                for item in items:
                    if item.endswith("/"):
                        folders.append(item)
                    else:
                        files.append(item)
                
                # Afficher les dossiers
                st.subheader("📂 Dossiers")
                for folder in folders:
                    col1, col2 = st.columns([6, 1])
                    with col1:
                        st.write(f"📁 {folder}")
                    with col2:
                        if st.button("Ouvrir", key=f"open_{folder}"):
                            st.session_state.current_path = folder
                            st.rerun()
                
                # Afficher les fichiers
                st.subheader("📄 Fichiers")
                for file in files:
                    col1, col2, col3 = st.columns([6, 2, 2])
                    with col1:
                        st.write(f"📄 {file.split('/')[-1]}")
                    with col2:
                        if st.button("Aperçu", key=f"view_{file}"):
                            try:
                                df = client.read_parquet_head(file, 10)
                                st.dataframe(df)
                            except:
                                st.warning("Format non supporté pour l'aperçu")
                    with col3:
                        if file.endswith(".parquet"):
                            if st.button("Télécharger", key=f"dl_{file}"):
                                # Logique de téléchargement
                                st.info("Téléchargement simulé - à implémenter")
            else:
                st.info("📭 Dossier vide")
                
        except Exception as e:
            st.error(f"Erreur : {e}")

# Structure pré-définie
st.markdown("---")
st.subheader("🏗️ Structure standard du DataLake")

structure = {
    "RAW (Données brutes)": [
        "/hadoop-climate-risk/raw/noaa/",
        "/hadoop-climate-risk/raw/usgs/"
    ],
    "SILVER (Nettoyées)": [
        "/hadoop-climate-risk/silver/noaa_cleaned/",
        "/hadoop-climate-risk/silver/usgs_cleaned/"
    ],
    "GOLD (Agrégées)": [
        "/hadoop-climate-risk/gold/daily_aggregates/",
        "/hadoop-climate-risk/gold/monthly_trends/"
    ],
    "ALERTES": [
        "/hadoop-climate-risk/alerts/"
    ]
}

for category, paths in structure.items():
    with st.expander(f"**{category}**"):
        for path in paths:
            if st.button(f"📂 Ouvrir {path.split('/')[-2]}", key=path):
                st.session_state.current_path = path
                st.rerun()
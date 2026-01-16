FROM python:3.9-slim

WORKDIR /app

# Installation des dépendances système
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    netcat-traditional \
    && rm -rf /var/lib/apt/lists/*

# Copie des requirements
COPY requirements.txt .

# Installation Python
RUN pip install --no-cache-dir -r requirements.txt

# Installation HDFS et Kafka
RUN pip install --no-cache-dir \
    pyarrow==14.0.1 \
    kafka-python==2.0.2 \
    hdfs3==0.3.1

# Copie de l'application
COPY frontend/ .

EXPOSE 8501

CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
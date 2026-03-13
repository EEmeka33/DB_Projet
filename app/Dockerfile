FROM python:3.11

WORKDIR /app

# Dépendances
COPY requirements.txt .
RUN pip install -r requirements.txt

# Code + script d'entrypoint
COPY . .

# Script de démarrage: imports puis Streamlit
ENTRYPOINT ["./entrypoint.sh"]

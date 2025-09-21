# Użyj oficjalnego obrazu Playwrighta
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Ustaw katalog roboczy
WORKDIR /app

# Skopiuj wszystkie pliki do kontenera
COPY . .

# Instalacja zależności Pythona
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# Instalacja przeglądarek Playwright (UWAGA! Zmiana tutaj!)
RUN python -m playwright install

# Komenda startowa
CMD ["python", "main.py"]

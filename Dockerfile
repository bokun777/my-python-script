# Użyj oficjalnego obrazu Playwrighta
FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Ustaw katalog roboczy
WORKDIR /app

# Skopiuj pliki do kontenera
COPY . .

# Zainstaluj zależności
RUN pip install --upgrade pip \
 && pip install -r requirements.txt

# Zainstaluj przeglądarki Playwright
RUN playwright install

# Uruchom aplikację
CMD ["python", "main.py"]

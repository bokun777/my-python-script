FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

# Ustaw katalog roboczy
WORKDIR /app

# Kopiuj zależności i zainstaluj
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Kopiuj cały kod
COPY . .

# Domyślne polecenie
CMD ["python", "main.py"]

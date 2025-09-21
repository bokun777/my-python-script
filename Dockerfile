FROM python:3.10-slim

# Instalacja systemowych zależności do Playwrighta
RUN apt-get update && apt-get install -y \
    wget gnupg ca-certificates fonts-liberation libappindicator3-1 \
    libasound2 libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 \
    libgdk-pixbuf-2.0-0 libnspr4 libnss3 libx11-xcb1 libxcomposite1 \
    libxdamage1 libxrandr2 xdg-utils libgbm1 libgtk-3-0 libxshmfence1 \
    libxkbcommon0 libxcb1 libx11-6 libxext6 libxfixes3 libxrender1 \
    libatspi2.0-0 libexpat1 libxrandr2 libnss3-tools && \
    rm -rf /var/lib/apt/lists/*

# Instalacja Playwright i pobranie przeglądarek
RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt
RUN python -m playwright install

# Kopiujemy cały projekt do kontenera
COPY . /app
WORKDIR /app

# Domyślna komenda uruchamiająca Twój skrypt
CMD ["python", "main.py"]

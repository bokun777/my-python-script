# 1. Oficjalny obraz Playwrighta z przeglądarkami
FROM mcr.microsoft.com/playwright/python:v1.55.0-beta-1756314050000-jammy

# 2. Ustaw katalog roboczy
WORKDIR /app

# 3. Skopiuj pliki z projektu
COPY . .

# 4. Zainstaluj wymagane biblioteki (w tym playwright!)
RUN pip install --upgrade pip \
 && pip install playwright \
 && pip install -r requirements.txt

# 5. Zainstaluj przeglądarki Playwright
RUN python -m playwright install

# 6. Uruchom aplikację
CMD ["python", "main.py"]

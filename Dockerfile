FROM mcr.microsoft.com/playwright/python:v1.44.0-jammy

WORKDIR /app

COPY . .

RUN pip install --upgrade pip \
 && pip install playwright==1.44.0 \
 && pip install -r requirements.txt

RUN python -m playwright install

CMD ["python", "main.py"]

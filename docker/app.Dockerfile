# Lightweight image for the data generator and the dashboard
FROM python:3.10-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
ENV PYTHONPATH=/app PYTHONUNBUFFERED=1
COPY . /app

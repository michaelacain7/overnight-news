FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY overnight_news_monitor.py .

CMD ["python", "overnight_news_monitor.py", "--schedule"]

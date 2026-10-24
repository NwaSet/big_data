FROM python:3.13-slim

RUN pip install --no-cache-dir requests confluent-kafka

WORKDIR /app

COPY producer.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "producer.py"]
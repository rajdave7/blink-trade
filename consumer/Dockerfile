FROM python:3.11-slim
WORKDIR /app
COPY kafka_consumer.py /app/
RUN pip install confluent-kafka
CMD ["python", "kafka_consumer.py"]
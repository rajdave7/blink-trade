FROM python:3.11-slim
WORKDIR /app
COPY decoder_producer.py /app/
RUN pip install confluent-kafka
CMD ["python", "decoder_producer.py"]

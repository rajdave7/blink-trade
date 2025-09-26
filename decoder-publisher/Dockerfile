FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/
RUN pip install -r requirements.txt
COPY decoder_producer.py /app/
EXPOSE 8000
CMD ["python", "decoder_producer.py"]
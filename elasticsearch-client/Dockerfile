FROM python:3.9-slim

WORKDIR /app

ENV ELASTICSEARCH_VERSION 8.12.1

RUN pip install elasticsearch==$ELASTICSEARCH_VERSION

COPY client.py .

CMD ["python", "client.py"]

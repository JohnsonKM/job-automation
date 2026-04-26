FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install redis psycopg2-binary

CMD ["python", "-m", "services.worker.worker"]

FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install --upgrade pip \
    && pip install redis

CMD ["python", "-m", "services.scraper.scraper"]

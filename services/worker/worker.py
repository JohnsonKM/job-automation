import redis, json, time
import psycopg2
from shared.db import get_conn

MAX_FIELD_LENGTHS = {
    "title": 500,
    "company": 500,
    "location": 500,
    "url": 2000,
    "description": 10000,
    "source": 200,
}

def get_redis_client(delay=3):
    while True:
        try:
            client = redis.Redis(host="redis", port=6379, decode_responses=True)
            client.ping()
            return client
        except redis.exceptions.ConnectionError:
            time.sleep(delay)

def normalize_job(raw_job):
    if not isinstance(raw_job, dict):
        return None

    job = {}
    for field, max_length in MAX_FIELD_LENGTHS.items():
        value = raw_job.get(field, "")
        if value is None:
            value = ""
        if not isinstance(value, str):
            value = str(value)
        value = value.strip()
        if field in {"title", "company", "location", "url", "source"} and not value:
            return None
        job[field] = value[:max_length]

    return job

r = get_redis_client()

while True:
    try:
        job = r.lpop("jobs_raw")
    except redis.exceptions.ConnectionError:
        time.sleep(3)
        r = get_redis_client()
        continue

    if not job:
        time.sleep(2)
        continue

    try:
        job = json.loads(job)
    except json.JSONDecodeError:
        time.sleep(1)
        continue

    job = normalize_job(job)
    if job is None:
        time.sleep(1)
        continue

    delay = 1

    while True:
        conn = None
        cur = None
        try:
            conn = get_conn()
            cur = conn.cursor()

            cur.execute("""
                INSERT INTO jobs (title, company, location, url, description, source)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT (url) DO NOTHING
            """, (
                job["title"],
                job["company"],
                job["location"],
                job["url"],
                job["description"],
                job["source"]
            ))

            conn.commit()
            print("Processed:", job["title"])
            break
        except psycopg2.OperationalError:
            time.sleep(delay)
            delay = min(delay * 2, 30)
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

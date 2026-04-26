import redis, json, time
from .sources import get_jobs

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
    jobs = get_jobs()

    try:
        for raw_job in jobs:
            job = normalize_job(raw_job)
            if job is None:
                continue
            r.rpush("jobs_raw", json.dumps(job))
    except redis.exceptions.ConnectionError:
        time.sleep(3)
        r = get_redis_client()
        continue

    print("Scraped jobs...")
    time.sleep(30)

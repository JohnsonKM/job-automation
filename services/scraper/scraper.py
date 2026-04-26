import json
import logging
import random
import socket
import time
import uuid

import redis
from .sources import get_jobs

MAX_FIELD_LENGTHS = {
    "title": 500,
    "company": 500,
    "location": 500,
    "url": 2000,
    "description": 10000,
    "source": 200,
}
MAX_FETCHED_JOBS = 500
MAX_REDIS_CONNECT_ATTEMPTS = 5
SCRAPE_INTERVAL_SECONDS = 30
RAW_QUEUE_KEY = "jobs_raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("scraper")

def sleep_with_backoff(delay, max_delay=30):
    sleep_for = min(delay, max_delay) + random.uniform(0, 1)
    time.sleep(sleep_for)
    return min(delay * 2, max_delay)

def get_redis_client(max_attempts=MAX_REDIS_CONNECT_ATTEMPTS, delay=1):
    retry_delay = delay
    for attempt in range(max_attempts):
        try:
            client = redis.Redis(host="redis", port=6379, decode_responses=True)
            client.ping()
            if attempt:
                logger.info("Connected to Redis after retry")
            else:
                logger.info("Connected to Redis")
            return client
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.warning(
                "Redis unavailable, retrying in %.1fs (attempt %s/%s)",
                retry_delay,
                attempt + 1,
                max_attempts,
            )
            retry_delay = sleep_with_backoff(retry_delay)
    logger.error("Redis unavailable after %s attempts", max_attempts)
    return None

def collect_jobs(fetched):
    if fetched is None:
        return []
    if isinstance(fetched, (str, bytes, dict)):
        logger.warning("Ignoring malformed jobs payload type: %s", type(fetched).__name__)
        return []

    try:
        iterator = iter(fetched)
    except TypeError:
        logger.warning("Ignoring non-iterable jobs payload type: %s", type(fetched).__name__)
        return []

    jobs = []
    for index, item in enumerate(iterator):
        if index >= MAX_FETCHED_JOBS:
            logger.warning("Truncating fetched jobs at %s items", MAX_FETCHED_JOBS)
            break
        jobs.append(item)
    return jobs

def make_queue_payload(job):
    payload = dict(job)
    payload["_queue_id"] = uuid.uuid4().hex
    payload["_queue_retries"] = 0
    return json.dumps(payload, separators=(",", ":"))

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

r = None

while True:
    if r is None:
        r = get_redis_client()
        if r is None:
            time.sleep(SCRAPE_INTERVAL_SECONDS)
            continue

    jobs = []
    pushed = 0
    try:
        jobs = collect_jobs(get_jobs())
        logger.info("Fetched %s job(s)", len(jobs))

        batch_urls = set()
        for raw_job in jobs:
            job = normalize_job(raw_job)
            if job is None:
                logger.warning("Skipping invalid scraped job payload")
                continue
            url = job["url"]
            if url in batch_urls:
                continue
            batch_urls.add(url)
            r.rpush(RAW_QUEUE_KEY, make_queue_payload(job))
            pushed += 1
        logger.info("Queued %s job(s) to Redis", pushed)
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
        logger.warning("Redis write failed, reconnecting")
        logger.info("Queued %s job(s) to Redis before reconnect", pushed)
        r = None
        continue
    except Exception:
        logger.exception("Scraper loop failed")
        logger.info("Fetched %s job(s)", len(jobs))
        logger.info("Queued %s job(s) to Redis", pushed)
        time.sleep(3)
        continue

    time.sleep(SCRAPE_INTERVAL_SECONDS)

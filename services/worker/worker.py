import hashlib
import json
import logging
import random
import socket
import time

import psycopg2
import redis

from shared.db import get_conn

RAW_QUEUE_KEY = "jobs_raw"
PROCESSING_QUEUE_KEY = "jobs_processing"
DEAD_LETTER_QUEUE_KEY = "jobs_dead_letter"
MAX_FIELD_LENGTHS = {
    "title": 500,
    "company": 500,
    "location": 500,
    "url": 2000,
    "description": 10000,
    "source": 200,
}
MAX_DB_RETRIES = 5
MAX_QUEUE_RETRIES = 3
MAX_JOB_PAYLOAD_BYTES = 65536
MAX_REDIS_CONNECT_ATTEMPTS = 5

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("worker")

def sleep_with_backoff(delay, max_delay=30):
    sleep_for = min(delay, max_delay) + random.uniform(0, 1)
    time.sleep(sleep_for)
    return min(delay * 2, max_delay)

def sanitize_for_log(value, max_length=200):
    text = str(value).replace("\r", " ").replace("\n", " ").replace("\t", " ")
    text = "".join(ch for ch in text if ch.isprintable())
    if len(text) > max_length:
        return text[:max_length] + "..."
    return text

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

def move_next_job(client):
    return client.execute_command(
        "BLMOVE",
        RAW_QUEUE_KEY,
        PROCESSING_QUEUE_KEY,
        "LEFT",
        "RIGHT",
        2,
    )

def requeue_job(client, raw_job, retry_count):
    payload = dict(raw_job)
    payload["_queue_retries"] = retry_count
    client.rpush(RAW_QUEUE_KEY, json.dumps(payload, separators=(",", ":")))

def acknowledge_job(client, raw_payload):
    client.lrem(PROCESSING_QUEUE_KEY, 1, raw_payload)

def recover_inflight_jobs(client):
    recovered = 0
    while True:
        raw_payload = client.rpop(PROCESSING_QUEUE_KEY)
        if raw_payload is None:
            break
        client.lpush(RAW_QUEUE_KEY, raw_payload)
        recovered += 1
    if recovered:
        logger.warning("Recovered %s in-flight job(s) back to %s", recovered, RAW_QUEUE_KEY)

def dead_letter_job(client, raw_payload, reason):
    payload_hash = hashlib.sha256(raw_payload.encode("utf-8", errors="ignore")).hexdigest()
    record = {
        "reason": reason,
        "payload_sha256": payload_hash,
        "payload_preview": sanitize_for_log(raw_payload, max_length=500),
        "recorded_at": int(time.time()),
    }
    client.rpush(DEAD_LETTER_QUEUE_KEY, json.dumps(record, separators=(",", ":")))
    logger.error("Dead-lettered job payload: %s (%s)", payload_hash, reason)

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
            time.sleep(10)
            continue
        try:
            recover_inflight_jobs(r)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.warning("Failed to recover in-flight jobs, reconnecting")
            r = None
            continue

    raw_payload = None
    try:
        raw_payload = move_next_job(r)
    except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
        logger.warning("Redis read failed, reconnecting")
        r = None
        continue
    except Exception:
        logger.exception("Unexpected worker fetch failure")
        time.sleep(3)
        continue

    if raw_payload is None:
        continue

    logger.info("Fetched job payload from Redis")
    if len(raw_payload.encode("utf-8")) > MAX_JOB_PAYLOAD_BYTES:
        try:
            dead_letter_job(r, raw_payload, "oversized_payload")
            acknowledge_job(r, raw_payload)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.exception("Failed to dead-letter oversized payload")
            r = None
        time.sleep(1)
        continue

    try:
        raw_job = json.loads(raw_payload)
    except json.JSONDecodeError:
        try:
            dead_letter_job(r, raw_payload, "invalid_json")
            acknowledge_job(r, raw_payload)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.exception("Failed to dead-letter invalid JSON payload")
            r = None
        time.sleep(1)
        continue

    if not isinstance(raw_job, dict):
        try:
            dead_letter_job(r, raw_payload, "non_dict_payload")
            acknowledge_job(r, raw_payload)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.exception("Failed to dead-letter non-dict payload")
            r = None
        time.sleep(1)
        continue

    queue_retries = 0
    if isinstance(raw_job, dict):
        try:
            queue_retries = max(0, int(raw_job.get("_queue_retries", 0)))
        except (TypeError, ValueError):
            queue_retries = 0

    job = normalize_job(raw_job)
    if job is None:
        try:
            dead_letter_job(r, raw_payload, "invalid_job_schema")
            acknowledge_job(r, raw_payload)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.exception("Failed to dead-letter invalid job payload")
            r = None
        time.sleep(1)
        continue

    job_label = sanitize_for_log(job["title"])
    logger.info("Processing job: %s", job_label)
    delay = 1
    attempt = 0
    inserted = False

    while attempt < MAX_DB_RETRIES:
        attempt += 1
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
            inserted = True
            if cur.rowcount == 0:
                logger.info("Skipped duplicate job by URL: %s", job_label)
            else:
                logger.info("Inserted job successfully: %s", job_label)
            break
        except (psycopg2.OperationalError, psycopg2.InterfaceError, OSError, socket.timeout):
            logger.warning(
                "Postgres unavailable while inserting job, retrying in %.1fs (attempt %s/%s)",
                delay,
                attempt,
                MAX_DB_RETRIES,
            )
            delay = sleep_with_backoff(delay)
        except Exception:
            logger.exception("Unexpected worker insert failure for job: %s", job_label)
            break
        finally:
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()

    if inserted:
        try:
            acknowledge_job(r, raw_payload)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.exception("Failed to acknowledge processed job: %s", job_label)
            r = None
        time.sleep(0.1)
        continue

    if queue_retries < MAX_QUEUE_RETRIES:
        try:
            requeue_job(r, raw_job, queue_retries + 1)
            acknowledge_job(r, raw_payload)
            logger.warning(
                "Requeued job after DB failures: %s (queue retry %s/%s)",
                job_label,
                queue_retries + 1,
                MAX_QUEUE_RETRIES,
            )
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.exception("Failed to requeue job after DB failures: %s", job_label)
            r = None
    else:
        try:
            dead_letter_job(r, raw_payload, "db_retry_exhausted")
            acknowledge_job(r, raw_payload)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError, socket.timeout):
            logger.exception("Failed to dead-letter exhausted job: %s", job_label)
            r = None
        logger.error(
            "Dropping job after %s DB retries and %s queue retries: %s",
            MAX_DB_RETRIES,
            MAX_QUEUE_RETRIES,
            job_label,
        )
    time.sleep(1)

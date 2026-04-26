import logging
import os
import random
import socket
import time

import psycopg2

logger = logging.getLogger("shared.db")

def sleep_with_backoff(delay, max_delay=30):
    sleep_for = min(delay, max_delay) + random.uniform(0, 1)
    time.sleep(sleep_for)
    return min(delay * 2, max_delay)

def get_conn(retries=10, delay=3):
    retry_delay = delay
    for attempt in range(retries):
        try:
            return psycopg2.connect(
                host="postgres",
                database=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
        except (psycopg2.OperationalError, psycopg2.InterfaceError, OSError, socket.timeout):
            logger.warning(
                "Postgres connection attempt %s/%s failed, retrying in %.1fs",
                attempt + 1,
                retries,
                retry_delay,
            )
            if attempt == retries - 1:
                raise
            retry_delay = sleep_with_backoff(retry_delay)

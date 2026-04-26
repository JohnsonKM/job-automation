import os
import time

import psycopg2

def get_conn(retries=10, delay=3):
    for attempt in range(retries):
        try:
            return psycopg2.connect(
                host="postgres",
                database=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD")
            )
        except psycopg2.OperationalError:
            if attempt == retries - 1:
                raise
            time.sleep(delay)

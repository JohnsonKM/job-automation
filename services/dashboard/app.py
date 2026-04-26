from fastapi import FastAPI
import html
import logging
from pydantic import BaseModel
from shared.db import get_conn

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s"
)
logger = logging.getLogger("dashboard")

app = FastAPI()

class JobOut(BaseModel):
    title: str | None
    company: str | None
    location: str | None
    url: str | None
    description: str | None
    source: str | None

def escape_text(value):
    if value is None:
        return None
    return html.escape(str(value), quote=True)

@app.get("/jobs", response_model=list[JobOut])
def jobs():
    logger.info("Fetching recent jobs")
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT title, company, location, url, description, source
            FROM jobs
            ORDER BY id DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        return [
            {
                "title": escape_text(row[0]),
                "company": escape_text(row[1]),
                "location": escape_text(row[2]),
                "url": escape_text(row[3]),
                "description": escape_text(row[4]),
                "source": escape_text(row[5]),
            }
            for row in rows
        ]
    except Exception:
        logger.exception("Dashboard query failed")
        raise
    finally:
        logger.info("Dashboard query completed")
        cur.close()
        conn.close()

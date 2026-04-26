from fastapi import FastAPI
from pydantic import BaseModel
from shared.db import get_conn

app = FastAPI()

class JobOut(BaseModel):
    title: str | None
    company: str | None
    location: str | None
    url: str | None
    description: str | None
    source: str | None

@app.get("/jobs", response_model=list[JobOut])
def jobs():
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
                "title": row[0],
                "company": row[1],
                "location": row[2],
                "url": row[3],
                "description": row[4],
                "source": row[5],
            }
            for row in rows
        ]
    finally:
        cur.close()
        conn.close()

import argparse
import json
import os
import random
import subprocess
import threading
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import psycopg2
import redis


RAW_QUEUE_KEY = "jobs_raw"
PROCESSING_QUEUE_KEY = "jobs_processing"
DEAD_LETTER_QUEUE_KEY = "jobs_dead_letter"
COMPOSE_PROJECT_DIR = Path(__file__).resolve().parents[1]
ENV_PATH = COMPOSE_PROJECT_DIR / ".env"
DEFAULT_SETTLE_TIMEOUT = 180


@dataclass
class ScenarioResult:
    name: str
    passed: bool
    details: str


class ChaosHarness:
    def __init__(self) -> None:
        self.env = self._load_env()
        self.redis_client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=True)

    def _load_env(self) -> dict[str, str]:
        env = {}
        if ENV_PATH.exists():
            for line in ENV_PATH.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                env[key] = value
        return env

    def compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            ["docker", "compose", *args],
            cwd=COMPOSE_PROJECT_DIR,
            check=check,
            text=True,
            capture_output=True,
        )

    def compose_output(self, *args: str) -> str:
        return self.compose(*args).stdout.strip()

    def db_conn(self):
        return psycopg2.connect(
            host="127.0.0.1",
            port=5432,
            database=self.env["POSTGRES_DB"],
            user=self.env["POSTGRES_USER"],
            password=self.env["POSTGRES_PASSWORD"],
        )

    def ensure_stack(self, workers: int = 1) -> None:
        args = ["up", "-d", "--build"]
        if workers != 1:
            args.extend(["--scale", f"worker={workers}"])
        self.compose(*args)
        self.wait_for_healthy()

    def wait_for_healthy(self, timeout: int = 120) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            ps = self.compose_output("ps", "--format", "json")
            if ps:
                services = [json.loads(line) for line in ps.splitlines()]
                statuses = {row["Service"]: row["Health"] or row["State"] for row in services}
                if statuses.get("postgres") == "healthy" and statuses.get("redis") == "healthy":
                    return
            time.sleep(2)
        raise TimeoutError("Compose services did not become healthy")

    def restart_service(self, service: str) -> None:
        self.compose("restart", service)

    def stop_service(self, service: str) -> None:
        self.compose("stop", service)

    def start_service(self, service: str, workers: int | None = None) -> None:
        args = ["up", "-d"]
        if workers is not None:
            args.extend(["--scale", f"{service}={workers}"])
        args.append(service)
        self.compose(*args)

    def kill_service(self, service: str, signal: str = "SIGKILL") -> None:
        self.compose("kill", "-s", signal, service)

    def reset_state(self) -> None:
        self.ensure_stack()
        self.redis_client.delete(RAW_QUEUE_KEY, PROCESSING_QUEUE_KEY, DEAD_LETTER_QUEUE_KEY)
        with self.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE jobs RESTART IDENTITY")
            conn.commit()

    def queue_lengths(self) -> dict[str, int]:
        return {
            RAW_QUEUE_KEY: self.redis_client.llen(RAW_QUEUE_KEY),
            PROCESSING_QUEUE_KEY: self.redis_client.llen(PROCESSING_QUEUE_KEY),
            DEAD_LETTER_QUEUE_KEY: self.redis_client.llen(DEAD_LETTER_QUEUE_KEY),
        }

    def db_urls(self) -> list[str]:
        with self.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT url FROM jobs ORDER BY id ASC")
                return [row[0] for row in cur.fetchall()]

    def db_count(self) -> int:
        with self.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM jobs")
                return cur.fetchone()[0]

    def duplicate_url_count(self) -> int:
        with self.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT COUNT(*) FROM (
                        SELECT url
                        FROM jobs
                        GROUP BY url
                        HAVING COUNT(*) > 1
                    ) dupes
                    """
                )
                return cur.fetchone()[0]

    def inject_job(self, url: str, **overrides: str) -> str:
        payload = {
            "title": overrides.get("title", f"Job for {url}"),
            "company": overrides.get("company", "Demo"),
            "location": overrides.get("location", "Remote"),
            "url": url,
            "description": overrides.get("description", "Chaos test payload"),
            "source": overrides.get("source", "chaos"),
            "_queue_id": uuid.uuid4().hex,
            "_queue_retries": int(overrides.get("_queue_retries", 0)),
        }
        raw = json.dumps(payload, separators=(",", ":"))
        self.redis_client.rpush(RAW_QUEUE_KEY, raw)
        return raw

    def inject_raw_payload(self, payload: str, queue: str = RAW_QUEUE_KEY) -> None:
        self.redis_client.rpush(queue, payload)

    def await_convergence(
        self,
        expected_unique_urls: set[str],
        timeout: int = DEFAULT_SETTLE_TIMEOUT,
        allow_dead_letter: bool = True,
    ) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            db_urls = self.db_urls()
            db_set = set(db_urls)
            queue_lengths = self.queue_lengths()
            if (
                db_set == expected_unique_urls
                and len(db_urls) == len(expected_unique_urls)
                and self.duplicate_url_count() == 0
                and queue_lengths[RAW_QUEUE_KEY] == 0
                and queue_lengths[PROCESSING_QUEUE_KEY] == 0
                and (allow_dead_letter or queue_lengths[DEAD_LETTER_QUEUE_KEY] == 0)
            ):
                return
            time.sleep(2)
        raise AssertionError(
            f"System did not converge. DB={len(self.db_urls())} raw={self.redis_client.llen(RAW_QUEUE_KEY)} "
            f"processing={self.redis_client.llen(PROCESSING_QUEUE_KEY)} dlq={self.redis_client.llen(DEAD_LETTER_QUEUE_KEY)}"
        )

    def assert_clean_state(self, expected_unique_urls: set[str], allow_dead_letter: bool = True) -> None:
        db_urls = self.db_urls()
        queue_lengths = self.queue_lengths()
        assert len(db_urls) == len(expected_unique_urls), f"DB count mismatch: {len(db_urls)} != {len(expected_unique_urls)}"
        assert set(db_urls) == expected_unique_urls, "DB URL set mismatch"
        assert self.duplicate_url_count() == 0, "Duplicate URLs exist in DB"
        assert queue_lengths[RAW_QUEUE_KEY] == 0, "Raw queue not empty"
        assert queue_lengths[PROCESSING_QUEUE_KEY] == 0, "Processing queue not empty"
        if not allow_dead_letter:
            assert queue_lengths[DEAD_LETTER_QUEUE_KEY] == 0, "Dead letter queue not empty"

    def scenario_worker_crash_during_processing(self) -> ScenarioResult:
        name = "worker_crash_during_processing"
        self.reset_state()
        expected = {f"https://example.com/jobs/{i}" for i in range(100)}
        for url in expected:
            self.inject_job(url)

        killed = False
        deadline = time.time() + 30
        while time.time() < deadline:
            processing = self.redis_client.llen(PROCESSING_QUEUE_KEY)
            count = self.db_count()
            if processing > 0 and 0 < count < len(expected):
                self.kill_service("worker")
                killed = True
                break
            time.sleep(0.2)
        if not killed:
            return ScenarioResult(name, False, "Failed to catch worker mid-processing")

        self.start_service("worker")
        self.await_convergence(expected)
        self.assert_clean_state(expected)
        return ScenarioResult(name, True, "All 100 jobs processed exactly once after crash/restart")

    def scenario_redis_restart_mid_queue(self) -> ScenarioResult:
        name = "redis_restart_mid_queue"
        self.reset_state()
        expected = set()
        stop = threading.Event()

        def producer() -> None:
            for i in range(200):
                if stop.is_set():
                    break
                url = f"https://example.com/redis-restart/{i}"
                expected.add(url)
                try:
                    self.inject_job(url)
                except redis.RedisError:
                    pass
                time.sleep(0.02)

        thread = threading.Thread(target=producer, daemon=True)
        thread.start()
        time.sleep(1)
        self.restart_service("redis")
        self.wait_for_healthy()
        thread.join(timeout=15)
        stop.set()
        self.await_convergence(expected)
        self.assert_clean_state(expected)
        return ScenarioResult(name, True, "Queue recovered after Redis restart with no lost jobs")

    def scenario_postgres_transient_failure(self) -> ScenarioResult:
        name = "postgres_transient_failure"
        self.reset_state()
        expected = {f"https://example.com/postgres/{i}" for i in range(50)}
        for url in expected:
            self.inject_job(url)

        time.sleep(1)
        self.stop_service("postgres")
        time.sleep(8)
        self.start_service("postgres")
        self.wait_for_healthy()
        self.await_convergence(expected)
        self.assert_clean_state(expected)
        return ScenarioResult(name, True, "Transient Postgres outage recovered without loss or duplicates")

    def scenario_duplicate_job_injection(self) -> ScenarioResult:
        name = "duplicate_job_injection"
        self.reset_state()
        unique_urls = {
            "https://example.com/dupe/1",
            "https://example.com/dupe/2",
            "https://example.com/dupe/3",
        }
        for _ in range(20):
            for url in unique_urls:
                self.inject_job(url)
        self.kill_service("worker")
        time.sleep(1)
        self.start_service("worker")
        self.await_convergence(unique_urls)
        self.assert_clean_state(unique_urls)
        return ScenarioResult(name, True, "DB uniqueness prevented duplicate inserts under replay")

    def scenario_malformed_payloads(self) -> ScenarioResult:
        name = "malformed_payloads"
        self.reset_state()
        valid_url = "https://example.com/valid-after-malformed"
        oversized = "x" * (70 * 1024)
        self.inject_raw_payload("{bad json")
        self.inject_raw_payload(json.dumps(["not", "a", "dict"]))
        self.inject_raw_payload(oversized)
        self.inject_job(valid_url)
        self.await_convergence({valid_url})
        self.assert_clean_state({valid_url}, allow_dead_letter=True)
        assert self.redis_client.llen(DEAD_LETTER_QUEUE_KEY) >= 3, "Malformed payloads were not dead-lettered"
        return ScenarioResult(name, True, "Malformed payloads dead-lettered and valid job still processed")

    def scenario_post_commit_pre_ack_recovery(self) -> ScenarioResult:
        name = "post_commit_pre_ack_recovery"
        self.reset_state()
        url = "https://example.com/post-commit-pre-ack"
        raw = json.dumps(
            {
                "title": "Recovered job",
                "company": "Demo",
                "location": "Remote",
                "url": url,
                "description": "Synthetic in-flight job",
                "source": "chaos",
                "_queue_id": uuid.uuid4().hex,
                "_queue_retries": 0,
            },
            separators=(",", ":"),
        )
        with self.db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO jobs (title, company, location, url, description, source)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (url) DO NOTHING
                    """,
                    ("Recovered job", "Demo", "Remote", url, "Synthetic in-flight job", "chaos"),
                )
            conn.commit()
        self.inject_raw_payload(raw, queue=PROCESSING_QUEUE_KEY)
        self.restart_service("worker")
        self.await_convergence({url})
        self.assert_clean_state({url})
        return ScenarioResult(name, True, "Recovered in-flight committed job without duplicate insert")

    def scenario_high_load_stress(self) -> ScenarioResult:
        name = "high_load_stress"
        self.reset_state()
        self.start_service("worker", workers=3)
        expected = {f"https://example.com/stress/{i}" for i in range(10000)}
        for url in expected:
            self.inject_job(url)

        for _ in range(3):
            time.sleep(2)
            self.restart_service("redis")
            self.wait_for_healthy()

        self.await_convergence(expected, timeout=600)
        self.assert_clean_state(expected)
        return ScenarioResult(name, True, "High-load run converged with multiple workers and Redis restarts")

    def run_all(self) -> list[ScenarioResult]:
        scenarios = [
            self.scenario_worker_crash_during_processing,
            self.scenario_redis_restart_mid_queue,
            self.scenario_postgres_transient_failure,
            self.scenario_duplicate_job_injection,
            self.scenario_malformed_payloads,
            self.scenario_post_commit_pre_ack_recovery,
            self.scenario_high_load_stress,
        ]
        results = []
        for scenario in scenarios:
            try:
                results.append(scenario())
            except Exception as exc:  # noqa: BLE001
                results.append(ScenarioResult(scenario.__name__, False, str(exc)))
        return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Chaos test harness for the job automation pipeline")
    parser.add_argument(
        "--scenario",
        choices=[
            "all",
            "worker_crash_during_processing",
            "redis_restart_mid_queue",
            "postgres_transient_failure",
            "duplicate_job_injection",
            "malformed_payloads",
            "post_commit_pre_ack_recovery",
            "high_load_stress",
        ],
        default="all",
    )
    args = parser.parse_args()

    harness = ChaosHarness()
    if args.scenario == "all":
        results = harness.run_all()
    else:
        method = getattr(harness, f"scenario_{args.scenario}")
        results = [method()]

    failed = False
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name}: {result.details}")
        failed = failed or not result.passed
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())

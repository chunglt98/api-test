"""
ZOA Rate Limit Stress Tester — FastAPI Backend
Run: uvicorn server:app --reload --port 8000
"""

import asyncio
import json
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# ── State ────────────────────────────────────────────────────────────────────

class TestState:
    def __init__(self):
        self.running: bool = False
        self.results: deque = deque(maxlen=10_000)   # ring buffer
        self.start_time: float = 0
        self.config: dict = {}
        self.stop_event: asyncio.Event = asyncio.Event()

    def reset(self):
        self.running = False
        self.results.clear()
        self.start_time = 0
        self.config = {}
        self.stop_event = asyncio.Event()

state = TestState()


# ── Models ───────────────────────────────────────────────────────────────────

class TestConfig(BaseModel):
    url: str
    method: str = "GET"
    headers: dict[str, str] = {}
    body: str | None = None          # raw JSON string or None
    concurrency: int = 10            # number of parallel workers
    duration_sec: int = 30           # how long to run (seconds)
    delay_ms: int = 0                # optional delay between each worker's requests (ms)


# ── App ──────────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    state.stop_event.set()

app = FastAPI(title="ZOA Rate Limit Tester", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Worker logic ─────────────────────────────────────────────────────────────

def _is_rate_limited(status: int, body: str) -> bool:
    """Detect rate limit responses — 429 or ZOA-specific error codes."""
    if status == 429:
        return True
    # ZOA returns 200 with error field in some endpoints
    try:
        data = json.loads(body)
        error = data.get("error", data.get("error_code", 0))
        if isinstance(error, int) and error in (-216, -201, -209, 216, 201, 209):
            return True
        if isinstance(error, str) and "rate" in error.lower():
            return True
    except Exception:
        pass
    return False


async def _worker(client: httpx.AsyncClient, cfg: TestConfig, worker_id: int):
    """Single async worker — sends requests until stop_event is set."""
    method = cfg.method.upper()
    headers = cfg.headers
    content = cfg.body.encode() if cfg.body else None

    while not state.stop_event.is_set():
        ts = time.time()
        status = 0
        latency = 0.0
        rate_limited = False
        error_msg = ""

        try:
            resp = await client.request(
                method,
                cfg.url,
                headers=headers,
                content=content,
                timeout=10.0,
            )
            status = resp.status_code
            latency = resp.elapsed.total_seconds() * 1000  # ms
            rate_limited = _is_rate_limited(status, resp.text)
        except httpx.TimeoutException:
            status = 0
            error_msg = "timeout"
        except Exception as e:
            status = 0
            error_msg = str(e)[:80]

        state.results.append({
            "t": round(ts - state.start_time, 3),
            "status": status,
            "latency": round(latency, 1),
            "rate_limited": rate_limited,
            "worker": worker_id,
            "error": error_msg,
        })

        if cfg.delay_ms > 0:
            await asyncio.sleep(cfg.delay_ms / 1000)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.post("/test/start")
async def start_test(cfg: TestConfig):
    if state.running:
        return {"ok": False, "message": "Test already running"}

    state.reset()
    state.config = cfg.model_dump()
    state.running = True
    state.start_time = time.time()

    async def run():
        limits = httpx.Limits(max_connections=cfg.concurrency + 10,
                              max_keepalive_connections=cfg.concurrency)
        async with httpx.AsyncClient(limits=limits) as client:
            # schedule stop after duration
            async def _stopper():
                await asyncio.sleep(cfg.duration_sec)
                state.stop_event.set()
                state.running = False

            workers = [_worker(client, cfg, i) for i in range(cfg.concurrency)]
            await asyncio.gather(*workers, _stopper())

        state.running = False

    asyncio.create_task(run())
    return {"ok": True, "message": f"Test started — {cfg.concurrency} workers for {cfg.duration_sec}s"}


@app.post("/test/stop")
async def stop_test():
    state.stop_event.set()
    state.running = False
    return {"ok": True, "message": "Test stopped"}


@app.get("/test/status")
async def test_status():
    results = list(state.results)
    total = len(results)
    successes = sum(1 for r in results if 200 <= r["status"] < 300)
    rate_limited = sum(1 for r in results if r["rate_limited"])
    errors = sum(1 for r in results if r["status"] == 0)
    latencies = [r["latency"] for r in results if r["latency"] > 0]

    elapsed = round(time.time() - state.start_time, 1) if state.start_time else 0
    rps = round(total / elapsed, 1) if elapsed > 0 else 0

    # First rate-limited timestamp
    first_rl = next((r["t"] for r in results if r["rate_limited"]), None)

    # p50 / p95 latency
    p50 = p95 = 0.0
    if latencies:
        s = sorted(latencies)
        p50 = s[int(len(s) * 0.5)]
        p95 = s[int(len(s) * 0.95)]

    return {
        "running": state.running,
        "elapsed": elapsed,
        "total": total,
        "successes": successes,
        "rate_limited": rate_limited,
        "errors": errors,
        "rps": rps,
        "p50_ms": round(p50, 1),
        "p95_ms": round(p95, 1),
        "first_rl_at": first_rl,
    }


@app.get("/test/results")
async def get_results(offset: int = 0):
    """Return raw results from offset (for incremental polling)."""
    results = list(state.results)
    return {
        "total": len(results),
        "data": results[offset:],
    }


@app.get("/test/stream")
async def stream_status():
    """Server-Sent Events — pushes stats every second."""
    async def event_gen():
        while state.running or not state.stop_event.is_set():
            results = list(state.results)
            total = len(results)
            elapsed = round(time.time() - state.start_time, 1) if state.start_time else 0
            rps = round(total / elapsed, 1) if elapsed > 0 else 0

            # bucket by second
            buckets: dict[int, dict] = {}
            for r in results:
                sec = int(r["t"])
                b = buckets.setdefault(sec, {"ok": 0, "rl": 0, "err": 0})
                if r["rate_limited"]:
                    b["rl"] += 1
                elif 200 <= r["status"] < 300:
                    b["ok"] += 1
                else:
                    b["err"] += 1

            payload = {
                "running": state.running,
                "elapsed": elapsed,
                "total": total,
                "rps": rps,
                "buckets": buckets,
            }
            yield f"data: {json.dumps(payload)}\n\n"
            await asyncio.sleep(1)

        # final flush
        yield f"data: {json.dumps({'running': False, 'done': True})}\n\n"

    return StreamingResponse(event_gen(), media_type="text/event-stream")


# ── Serve dashboard ───────────────────────────────────────────────────────────

@app.get("/")
async def serve_dashboard():
    return FileResponse("dashboard.html")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("server:app", host="0.0.0.0", port=8000, reload=True)

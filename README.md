# ZOA Rate Limit Tester

Web-based stress test tool for Zalo Official Account (ZOA) APIs.  
Sends concurrent requests to any ZOA endpoint and visualizes rate limit behavior in real time.

---

## Features

- Configure any ZOA API endpoint (URL, method, headers, body)
- Set concurrency (parallel workers) and test duration
- Live chart: 2xx success vs 429 rate-limited vs errors — bucketed per second
- Auto-detects rate limit hits (HTTP 429 + ZOA-specific error codes)
- Highlights the exact moment the first rate limit occurs
- Export raw results as CSV

---

## Requirements

- Python 3.10+
- pip

---

## Setup

```bash
# 1. Clone the repo
git clone <your-repo-url>
cd api-test

# 2. (Recommended) Create a virtual environment
python -m venv venv
source venv/bin/activate      # Windows: venv\Scripts\activate

# 3. Install dependencies
pip install -r requirements.txt
```

---

## Run

```bash
uvicorn server:app --port 8000
```

Then open your browser at: **http://localhost:8000**

---

## Usage

| Field | Description |
|---|---|
| URL | ZOA API endpoint, e.g. `https://openapi.zalo.me/v2.0/oa/message` |
| Method | HTTP method (POST, GET, …) |
| Access Token | Your ZOA access token — entered as Bearer, stored in memory only |
| Extra Headers | Additional headers as JSON |
| Request Body | JSON body for POST requests |
| Concurrency | Number of parallel workers (default: 20) |
| Duration | How long to run the test in seconds (default: 30) |
| Delay (ms) | Optional delay between each worker's requests |

Click **Start Stress Test** to begin. The dashboard updates every second.

---

## Rate Limit Detection

The tool flags a response as rate-limited if:
- HTTP status is `429`
- Response body contains ZOA error codes: `-216`, `-201`, `-209`

---

## Security Note

Never commit your access token. Use the token field in the UI at runtime — it is never saved to disk.

---

## Project Structure

```
api-test/
├── server.py        # FastAPI backend — handles test execution & SSE stream
├── dashboard.html   # Single-page web UI
├── requirements.txt
└── .gitignore
```

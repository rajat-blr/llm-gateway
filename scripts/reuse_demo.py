"""Exercise opt-in exact caching/coalescing using one temporary local key."""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json
import os
import threading
import time
import urllib.request
import uuid

from openai import OpenAI

base = os.environ.get("GATEWAY_BASE_URL", "http://localhost:8080/v1").rstrip("/")
root = base.removesuffix("/v1")
admin_token = os.environ.get("GATEWAY_ADMIN_TOKEN", "demo-local-admin-token")


def admin(method, path, payload=None):
    request = urllib.request.Request(root + path, method=method, data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + admin_token})
    with urllib.request.urlopen(request, timeout=5) as response:
        data = response.read()
        return json.loads(data) if data else {}


created = admin("POST", "/admin/keys", {"name": "reuse-reviewer", "aliases": ["default-chat"], "capacity": 4096, "refill_per_second": 10, "max_output_tokens": 64, "allow_reuse": True})
try:
    with OpenAI(base_url=base, api_key=created["key"], max_retries=0) as client:
        def request(text):
            response = client.chat.completions.with_raw_response.create(model="default-chat", messages=[{"role": "user", "content": text}])
            completion = response.parse()
            assert completion.usage.total_tokens == 8
            return response.headers["X-Cache"], completion.id
        first = request("Exact replay demonstration")
        second = request("Exact replay demonstration")
        assert first[0] == "MISS" and second[0] == "HIT"
        assert first[1] != second[1]
        barrier = threading.Barrier(8)
        prompt = "Concurrent demonstration " + uuid.uuid4().hex
        def parallel(_):
            barrier.wait(timeout=5)
            return request(prompt)
        with ThreadPoolExecutor(max_workers=8) as executor:
            results = list(executor.map(parallel, range(8)))
        assert len({identity for _, identity in results}) == 8
        assert sum(state == "MISS" for state, _ in results) == 1
        deadline = time.monotonic() + 5
        while True:
            report = admin("GET", "/admin/usage?key_id=" + created["id"])
            if sum(row["requests"] for row in report["requests"]) == 10:
                break
            if time.monotonic() > deadline:
                raise AssertionError("Reuse usage did not persist")
            time.sleep(0.01)
        assert sum(row["attempts"] for row in report["attempts"]) == 2
        assert sum(row["output_tokens"] for row in report["attempts"]) == 6
        assert sum(row["client_output_tokens"] for row in report["requests"]) == 30
        print("Reuse demo: MISS then HIT; parallel states", dict(Counter(state for state, _ in results)))
        print("Usage verified: 10 client completions, 2 upstream attempts; fresh response IDs and separate client/provider usage")
finally:
    admin("DELETE", "/admin/keys/" + created["id"])

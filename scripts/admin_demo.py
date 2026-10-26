"""Create, exercise, inspect and revoke a temporary reviewer key.

Requires the running Compose demo and examples/requirements.txt. Never prints
or saves the one-time credential. Existing keys are not changed.
"""
import argparse
import json
import os
import time
import urllib.request

from openai import APIStatusError, OpenAI

base = os.environ.get("GATEWAY_BASE_URL", "http://localhost:8080/v1").rstrip("/")
root = base.removesuffix("/v1")
admin_token = os.environ.get("GATEWAY_ADMIN_TOKEN", "demo-local-admin-token")


def admin(method, path, payload=None):
    request = urllib.request.Request(
        root + path, method=method,
        data=json.dumps(payload).encode() if payload is not None else None,
        headers={"Content-Type": "application/json", "Authorization": "Bearer " + admin_token},
    )
    with urllib.request.urlopen(request, timeout=5) as response:
        data = response.read()
        return json.loads(data) if data else {}


parser = argparse.ArgumentParser()
parser.add_argument("--expect-previous", action="store_true", help="verify a prior reviewer key survived container recreation")
args = parser.parse_args()
if args.expect_previous:
    keys = admin("GET", "/admin/keys")["data"]
    assert any(key["name"] == "reviewer-demo" and key["revoked_ms"] is not None for key in keys), "Prior revoked key did not survive recreation"

created = admin("POST", "/admin/keys", {
    "name": "reviewer-demo", "aliases": ["default-chat"], "capacity": 256,
    "refill_per_second": 10, "max_output_tokens": 16, "allow_reuse": False,
})
try:
    with OpenAI(base_url=base, api_key=created["key"], max_retries=0) as client:
        response = client.chat.completions.create(model="default-chat", messages=[{"role": "user", "content": "Hello from the reviewer demo"}])
        assert response.usage.total_tokens == 8
        assert created["key"] not in json.dumps(admin("GET", "/admin/keys"))
        deadline = time.monotonic() + 3
        while True:
            report = admin("GET", "/admin/usage?key_id=" + created["id"])
            if report["attempts"]:
                break
            if time.monotonic() >= deadline:
                raise AssertionError("Usage did not persist within the demo deadline")
            time.sleep(0.01)
        assert sum(row["requests"] for row in report["requests"]) == 1
        assert sum(row["attempts"] for row in report["attempts"]) == 1
        admin("DELETE", "/admin/keys/" + created["id"])
        try:
            client.models.list()
        except APIStatusError as error:
            assert error.status_code == 401
        else:
            raise AssertionError("Revoked key still authenticated")
    print("Admin demo: key created, isolated usage persisted, immediate revocation verified")
finally:
    admin("DELETE", "/admin/keys/" + created["id"])

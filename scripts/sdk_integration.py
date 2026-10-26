"""Deterministic subprocess/SDK acceptance tests; requires make build first."""
import contextlib
from concurrent.futures import ThreadPoolExecutor
import threading
import signal
import json
import os
from pathlib import Path
import socket
import sqlite3
import subprocess
import tempfile
import time
import unittest
import uuid
import urllib.request
import urllib.error

from openai import APIError, APIStatusError, BadRequestError, OpenAI

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_until(check, timeout=5):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            if check():
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.01)
    raise AssertionError("condition did not become true within deadline")


def get_json(url):
    with urllib.request.urlopen(url, timeout=1) as response:
        return json.load(response)


def stop(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=2)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=2)
            raise AssertionError("process exceeded shutdown deadline")


@contextlib.contextmanager
def running(mock_args=(), overrides=None, fallback=False, require_ready=True, cache_ttl=None):
    mock_port, gateway_port, secondary_port = free_port(), free_port(), free_port()
    config = json.loads((ROOT / "configs/demo.json").read_text())
    config["listen"] = f"127.0.0.1:{gateway_port}"
    for name, provider in config["providers"].items():
        port = secondary_port if name.startswith("backup") else mock_port
        provider["base_url"] = f"http://127.0.0.1:{port}/v1"
    if not fallback:
        for route in config["routes"].values():
            route["targets"] = route["targets"][:1]
    config["redis"]["address"] = os.environ.get("REDIS_TEST_ADDR", config["redis"]["address"])
    config["redis"]["prefix"] = "sdk:" + uuid.uuid4().hex
    config.update(overrides or {})
    if cache_ttl is not None:
        for route in config["routes"].values():
            route["cache"]["ttl"] = cache_ttl
    # Deadline scenarios intentionally shorten the complete budget.
    def seconds(value):
        return float(value[:-2])/1000 if value.endswith("ms") else float(value[:-1])
    for route in config["routes"].values():
        for target in route["targets"]:
            if seconds(target["attempt_timeout"]) > seconds(config["request_timeout"]):
                target["attempt_timeout"] = config["request_timeout"]
    if seconds(config["backoff_max"]) > seconds(config["request_timeout"]):
        config["backoff_max"] = config["request_timeout"]
    env = dict(os.environ, GATEWAY_API_KEY="sdk-test-key", GATEWAY_ADMIN_TOKEN="sdk-admin-test-token", MOCK_API_KEY="fixture")
    with tempfile.TemporaryDirectory(prefix="gateway-sdk-") as directory:
        config["sqlite"]["path"] = str(Path(directory) / "gateway.db")
        path = Path(directory) / "config.json"
        path.write_text(json.dumps(config))
        with open(Path(directory) / "logs.txt", "w+") as logs:
            mock = subprocess.Popen(
                [str(ROOT / "bin/mock"), "-listen", f"127.0.0.1:{mock_port}", *mock_args],
                stdout=logs, stderr=logs, env=env,
            )
            secondary = subprocess.Popen(
                [str(ROOT / "bin/mock"), "-listen", f"127.0.0.1:{secondary_port}"],
                stdout=logs, stderr=logs, env=env,
            )
            gateway = None
            client = None
            try:
                wait_until(lambda: get_json(f"http://127.0.0.1:{mock_port}/stats"))
                wait_until(lambda: get_json(f"http://127.0.0.1:{secondary_port}/stats"))
                gateway = subprocess.Popen(
                    [str(ROOT / "bin/gateway"), "-config", str(path)],
                    stdout=logs, stderr=logs, env=env,
                )
                wait_until(lambda: get_json(f"http://127.0.0.1:{gateway_port}/" + ("readyz" if require_ready else "healthz")))
                client = OpenAI(api_key="sdk-test-key", base_url=f"http://127.0.0.1:{gateway_port}/v1", max_retries=0, timeout=5)
                yield client, gateway, {"primary": f"http://127.0.0.1:{mock_port}/stats", "secondary": f"http://127.0.0.1:{secondary_port}/stats", "database": config["sqlite"]["path"], "config": str(path)}
            except Exception:
                logs.flush()
                logs.seek(0)
                print(logs.read())
                raise
            finally:
                if client is not None:
                    client.close()
                if gateway is not None:
                    stop(gateway)
                stop(mock)
                stop(secondary)


def admin(client, method, path, payload=None):
    base = str(client.base_url).rstrip("/").removesuffix("/v1")
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(base + path, data=data, method=method, headers={"Content-Type": "application/json", "Authorization": "Bearer sdk-admin-test-token"})
    with urllib.request.urlopen(request, timeout=2) as response:
        content = response.read()
        return json.loads(content) if content else {}


def reusable_client(client):
    created = admin(client, "POST", "/admin/keys", {"name": "sdk-reuse", "aliases": ["default-chat"], "capacity": 10000, "refill_per_second": 1, "max_output_tokens": 100, "allow_reuse": True})
    return created, OpenAI(base_url=str(client.base_url), api_key=created["key"], max_retries=0, timeout=5)


def raw_completion(client):
    response = client.chat.completions.with_raw_response.create(model="default-chat", messages=MESSAGES, max_tokens=32)
    return response.headers, response.parse()


MESSAGES = [{"role": "user", "content": "Hello"}]


class SDKAcceptance(unittest.TestCase):
    def test_both_adapters(self):
        with running() as (client, _, stats):
            self.assertEqual(len(client.models.list().data), 2)
            for alias in ("default-chat", "anthropic-chat"):
                result = client.chat.completions.create(model=alias, messages=MESSAGES, max_tokens=32)
                self.assertEqual(result.model, alias)
                self.assertEqual(result.choices[0].message.content, "hello hello hello ")
                self.assertEqual(result.usage.total_tokens, 8)
                with client.chat.completions.create(model=alias, messages=MESSAGES, stream=True, stream_options={"include_usage": True}) as stream:
                    chunks = list(stream)
                ids = {chunk.id for chunk in chunks}
                self.assertEqual(len(ids), 1)
                self.assertEqual("".join(chunk.choices[0].delta.content or "" for chunk in chunks if chunk.choices), "hello hello hello ")
                self.assertEqual(chunks[-1].usage.total_tokens, 8)
                self.assertEqual(chunks[-1].choices, [])
            self.assertEqual(get_json(stats["primary"])["calls"], 4)

    def test_unsupported_field_is_explicit(self):
        with running() as (client, _, stats):
            with self.assertRaises(BadRequestError) as error:
                client.chat.completions.create(model="default-chat", messages=MESSAGES, extra_body={"tools": []})
            self.assertEqual(error.exception.code, "invalid_request")
            self.assertEqual(get_json(stats["primary"])["calls"], 0)

    def test_midstream_error_surfaces_in_sdk(self):
        for alias in ("default-chat", "anthropic-chat"):
            with self.subTest(alias=alias), running(("-chunks", "5", "-drop-after", "1")) as (client, _, stats):
                chunks = []
                with self.assertRaises(APIError) as error:
                    with client.chat.completions.create(model=alias, messages=MESSAGES, stream=True) as stream:
                        for chunk in stream:
                            chunks.append(chunk)
                self.assertIn("upstream provider failed", str(error.exception).lower())
                self.assertTrue(chunks)
                self.assertEqual(get_json(stats["primary"])["calls"], 1)

    def test_failure_before_commit(self):
        with running(("-fail-every", "1")) as (client, _, _):
            with self.assertRaises(APIStatusError) as error:
                client.chat.completions.create(model="default-chat", messages=MESSAGES)
            self.assertEqual(error.exception.status_code, 502)
            self.assertEqual(error.exception.code, "upstream_failure")

    def test_disconnect_cancels_upstream(self):
        with running(("-chunks", "100", "-chunk-delay", "200ms")) as (client, _, stats):
            with client.chat.completions.create(model="default-chat", messages=MESSAGES, stream=True) as stream:
                next(stream)
            wait_until(lambda: get_json(stats["primary"])["canceled"] == 1, timeout=1)

    def test_request_deadline(self):
        overrides = {"request_timeout": "200ms", "idle_timeout": "200ms", "write_timeout": "100ms"}
        with running(("-first-delay", "2s"), overrides) as (client, _, _):
            with self.assertRaises(APIStatusError) as error:
                client.chat.completions.create(model="default-chat", messages=MESSAGES)
            # Overall and per-read deadlines coincide; either bounded failure
            # can win depending on scheduler timing.
            self.assertIn(error.exception.status_code, (502, 504))

    def test_transient_primary_falls_back(self):
        with running(("-fail-every", "1"), fallback=True) as (client, _, stats):
            result = client.chat.completions.create(model="default-chat", messages=MESSAGES)
            self.assertEqual(result.choices[0].message.content, "hello hello hello ")
            self.assertEqual(get_json(stats["primary"])["calls"], 1)
            self.assertEqual(get_json(stats["secondary"])["calls"], 1)

    def test_committed_stream_never_falls_back(self):
        with running(("-drop-after", "1"), fallback=True) as (client, _, stats):
            with self.assertRaises(APIError):
                with client.chat.completions.create(model="default-chat", messages=MESSAGES, stream=True) as stream:
                    list(stream)
            self.assertEqual(get_json(stats["primary"])["calls"], 1)
            self.assertEqual(get_json(stats["secondary"])["calls"], 0)

    def test_exhausted_bucket_is_explicit(self):
        with running(overrides={"token_limit": {"capacity": 1, "refill_per_second": 1}}) as (client, _, stats):
            with self.assertRaises(APIStatusError) as error:
                client.chat.completions.create(model="default-chat", messages=MESSAGES)
            self.assertEqual(error.exception.status_code, 429)
            self.assertEqual(error.exception.code, "rate_limit_exceeded")
            self.assertEqual(get_json(stats["primary"])["calls"], 0)

    def test_redis_unavailable_fails_closed(self):
        config = {"address": "127.0.0.1:1", "db": 0, "prefix": "outage", "timeout": "50ms", "reservation_ttl": "15m"}
        with running(overrides={"redis": config}, require_ready=False) as (client, _, stats):
            with self.assertRaises(APIStatusError) as error:
                client.chat.completions.create(model="default-chat", messages=MESSAGES)
            self.assertEqual(error.exception.status_code, 503)
            self.assertEqual(error.exception.code, "dependency_unavailable")
            self.assertEqual(get_json(stats["primary"])["calls"], 0)

    def test_admin_key_policy_and_immediate_revocation(self):
        with running() as (client, _, stats):
            policy = {"name": "sdk-created", "aliases": ["default-chat"], "capacity": 1000, "refill_per_second": 1, "max_output_tokens": 10, "allow_reuse": False}
            created = admin(client, "POST", "/admin/keys", policy)
            listed = admin(client, "GET", "/admin/keys")
            self.assertNotIn(created["key"], json.dumps(listed))
            with OpenAI(api_key=created["key"], base_url=str(client.base_url), max_retries=0) as limited:
                self.assertEqual([model.id for model in limited.models.list().data], ["default-chat"])
                with self.assertRaises(APIStatusError) as error:
                    limited.chat.completions.create(model="anthropic-chat", messages=MESSAGES)
                self.assertEqual(error.exception.status_code, 403)
                with self.assertRaises(BadRequestError):
                    limited.chat.completions.create(model="default-chat", messages=MESSAGES, max_tokens=11)
                result = limited.chat.completions.create(model="default-chat", messages=[{"role": "user", "content": "private-prompt-sentinel"}])
                self.assertEqual(result.usage.total_tokens, 8)
                admin(client, "DELETE", "/admin/keys/" + created["id"])
                with self.assertRaises(APIStatusError) as error:
                    limited.models.list()
                self.assertEqual(error.exception.status_code, 401)
            wait_until(lambda: admin(client, "GET", "/admin/status")["persistence"]["written_records"] >= 3)
            report = admin(client, "GET", "/admin/usage?key_id=" + created["id"])
            self.assertEqual(sum(row["attempts"] for row in report["attempts"]), 1)
            self.assertEqual(sum(row["requests"] for row in report["requests"]), 3)
            self.assertNotIn("private-prompt-sentinel", json.dumps(report))
            self.assertEqual(get_json(stats["primary"])["calls"], 1)

    def test_sqlite_write_contention_keeps_inference_available(self):
        with running() as (client, _, stats):
            lock = sqlite3.connect(stats["database"])
            try:
                lock.execute("BEGIN IMMEDIATE")
                for _ in range(3):
                    result = client.chat.completions.create(model="default-chat", messages=MESSAGES)
                    self.assertEqual(result.usage.total_tokens, 8)
                wait_until(lambda: admin(client, "GET", "/admin/status")["persistence"]["write_failures"] > 0)
                self.assertTrue(admin(client, "GET", "/admin/status")["sqlite_available"])
            finally:
                lock.rollback()
                lock.close()
            wait_until(lambda: (lambda s: s["enqueued_records"] == s["written_records"] + s["write_failures"])(admin(client, "GET", "/admin/status")["persistence"]))
            before = admin(client, "GET", "/admin/status")["persistence"]["written_records"]
            client.chat.completions.create(model="default-chat", messages=MESSAGES)
            wait_until(lambda: admin(client, "GET", "/admin/status")["persistence"]["written_records"] > before)

    def test_sqlite_persistence_and_revocation_across_restart(self):
        with running() as (client, gateway, _):
            client.chat.completions.create(model="default-chat", messages=MESSAGES)
            bootstrap = next(key for key in admin(client, "GET", "/admin/keys")["data"] if key["name"] == "bootstrap")
            admin(client, "DELETE", "/admin/keys/" + bootstrap["id"])
            gateway.terminate()
            gateway.wait(timeout=2)
            self.assertEqual(gateway.returncode, 0)
            env = dict(os.environ, GATEWAY_API_KEY="sdk-test-key", GATEWAY_ADMIN_TOKEN="sdk-admin-test-token", MOCK_API_KEY="fixture")
            restarted = subprocess.Popen(gateway.args, env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            try:
                base = str(client.base_url).rstrip("/").removesuffix("/v1")
                wait_until(lambda: get_json(base + "/readyz"))
                with self.assertRaises(APIStatusError) as error:
                    client.models.list()
                self.assertEqual(error.exception.status_code, 401)
                report = admin(client, "GET", "/admin/usage")
                self.assertEqual(sum(row["attempts"] for row in report["attempts"]), 1)
                self.assertEqual(sum(row["client_output_tokens"] for row in report["requests"]), 3)
            finally:
                stop(restarted)

    def test_exact_cache_and_attempt_accounting(self):
        with running() as (client, _, stats):
            key, reuse = reusable_client(client)
            with reuse:
                first_headers, first = raw_completion(reuse)
                second_headers, second = raw_completion(reuse)
                self.assertEqual(first_headers["X-Cache"], "MISS")
                self.assertEqual(second_headers["X-Cache"], "HIT")
                self.assertNotEqual(first.id, second.id)
                self.assertEqual(second.usage.total_tokens, 8)
            wait_until(lambda: admin(client, "GET", "/admin/status")["persistence"]["written_records"] == 3)
            report = admin(client, "GET", "/admin/usage?key_id=" + key["id"])
            self.assertEqual(sum(row["requests"] for row in report["requests"]), 2)
            self.assertEqual(sum(row["cache_hits"] for row in report["requests"]), 1)
            self.assertEqual(sum(row["attempts"] for row in report["attempts"]), 1)
            self.assertEqual(get_json(stats["primary"])["calls"], 1)

    def test_overlapping_requests_coalesce(self):
        with running(("-first-delay", "200ms")) as (client, _, stats):
            _, reuse = reusable_client(client)
            barrier = threading.Barrier(8)
            def request(_):
                barrier.wait(timeout=5)
                headers, completion = raw_completion(reuse)
                return headers["X-Cache"], completion.id
            with reuse, ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(request, range(8)))
            self.assertEqual(get_json(stats["primary"])["calls"], 1)
            self.assertEqual(sum(state == "MISS" for state, _ in results), 1)
            self.assertEqual(len({identity for _, identity in results}), 8)
            self.assertTrue(any(state == "COALESCED" for state, _ in results))
            self.assertEqual(admin(client, "GET", "/admin/status")["reuse"]["active_executions"], 0)

    def test_cache_ttl_expiry(self):
        with running(cache_ttl="100ms") as (client, _, stats):
            _, reuse = reusable_client(client)
            with reuse:
                self.assertEqual(raw_completion(reuse)[0]["X-Cache"], "MISS")
                time.sleep(0.15)
                self.assertEqual(raw_completion(reuse)[0]["X-Cache"], "MISS")
            self.assertEqual(get_json(stats["primary"])["calls"], 2)

    def test_sighup_rejects_invalid_and_publishes_revision(self):
        with running() as (client, gateway, stats):
            _, reuse = reusable_client(client)
            with reuse:
                raw_completion(reuse)
                self.assertEqual(raw_completion(reuse)[0]["X-Cache"], "HIT")
                path = Path(stats["config"])
                original = json.loads(path.read_text())
                invalid = json.loads(path.read_text())
                invalid["routes"]["default-chat"]["targets"][0]["provider"] = "missing"
                path.write_text(json.dumps(invalid))
                gateway.send_signal(signal.SIGHUP)
                wait_until(lambda: admin(client, "GET", "/admin/status")["reload_rejected"] == 1)
                headers, _ = raw_completion(reuse)
                self.assertEqual(headers["X-Cache"], "HIT")
                self.assertEqual(headers["X-Config-Generation"], "1")
                original["routes"]["default-chat"]["revision"] += 1
                original["routes"]["default-chat"]["targets"][0]["model"] = "mock-new-model"
                path.write_text(json.dumps(original))
                gateway.send_signal(signal.SIGHUP)
                wait_until(lambda: admin(client, "GET", "/admin/status")["config_generation"] == 2)
                headers, _ = raw_completion(reuse)
                self.assertEqual(headers["X-Cache"], "MISS")
                self.assertEqual(headers["X-Config-Generation"], "2")
                self.assertEqual(get_json(stats["primary"])["calls"], 2)
                self.assertIsNone(gateway.poll())

    def test_shutdown_drains_stream(self):
        with running(("-chunk-delay", "50ms")) as (client, gateway, _):
            with client.chat.completions.create(model="default-chat", messages=MESSAGES, stream=True) as stream:
                next(stream)
                gateway.terminate()
                chunks = list(stream)
            gateway.wait(timeout=2)
            self.assertEqual(gateway.returncode, 0)
            self.assertTrue(any(chunk.choices[0].finish_reason == "stop" for chunk in chunks if chunk.choices))

    def test_shutdown_cancels_after_budget(self):
        with running(("-chunks", "100", "-chunk-delay", "200ms"), {"shutdown_timeout": "200ms"}) as (client, gateway, stats):
            with client.chat.completions.create(model="default-chat", messages=MESSAGES, stream=True) as stream:
                next(stream)
                gateway.terminate()
                gateway.wait(timeout=2)
            self.assertNotEqual(gateway.returncode, 0)
            wait_until(lambda: get_json(stats["primary"])["canceled"] == 1, timeout=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

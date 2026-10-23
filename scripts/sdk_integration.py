"""Deterministic subprocess/SDK acceptance tests; requires make build first."""
import contextlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import unittest
import urllib.request

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
def running(mock_args=(), overrides=None):
    mock_port, gateway_port = free_port(), free_port()
    config = json.loads((ROOT / "configs/demo.json").read_text())
    config["listen"] = f"127.0.0.1:{gateway_port}"
    for provider in config["providers"].values():
        provider["base_url"] = f"http://127.0.0.1:{mock_port}/v1"
    config.update(overrides or {})
    env = dict(os.environ, GATEWAY_API_KEY="sdk-test-key", MOCK_API_KEY="fixture")
    with tempfile.TemporaryDirectory(prefix="gateway-sdk-") as directory:
        path = Path(directory) / "config.json"
        path.write_text(json.dumps(config))
        with open(Path(directory) / "logs.txt", "w+") as logs:
            mock = subprocess.Popen(
                [str(ROOT / "bin/mock"), "-listen", f"127.0.0.1:{mock_port}", *mock_args],
                stdout=logs, stderr=logs, env=env,
            )
            gateway = None
            client = None
            try:
                wait_until(lambda: get_json(f"http://127.0.0.1:{mock_port}/stats"))
                gateway = subprocess.Popen(
                    [str(ROOT / "bin/gateway"), "-config", str(path)],
                    stdout=logs, stderr=logs, env=env,
                )
                wait_until(lambda: get_json(f"http://127.0.0.1:{gateway_port}/readyz"))
                client = OpenAI(api_key="sdk-test-key", base_url=f"http://127.0.0.1:{gateway_port}/v1", max_retries=0, timeout=5)
                yield client, gateway, f"http://127.0.0.1:{mock_port}/stats"
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
            self.assertEqual(get_json(stats)["calls"], 4)

    def test_unsupported_field_is_explicit(self):
        with running() as (client, _, stats):
            with self.assertRaises(BadRequestError) as error:
                client.chat.completions.create(model="default-chat", messages=MESSAGES, extra_body={"tools": []})
            self.assertEqual(error.exception.code, "invalid_request")
            self.assertEqual(get_json(stats)["calls"], 0)

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
                self.assertEqual(get_json(stats)["calls"], 1)

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
            wait_until(lambda: get_json(stats)["canceled"] == 1, timeout=1)

    def test_request_deadline(self):
        overrides = {"request_timeout": "200ms", "idle_timeout": "200ms", "write_timeout": "100ms"}
        with running(("-first-delay", "2s"), overrides) as (client, _, _):
            with self.assertRaises(APIStatusError) as error:
                client.chat.completions.create(model="default-chat", messages=MESSAGES)
            # Overall and per-read deadlines coincide; either bounded failure
            # can win depending on scheduler timing.
            self.assertIn(error.exception.status_code, (502, 504))

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
            wait_until(lambda: get_json(stats)["canceled"] == 1, timeout=1)


if __name__ == "__main__":
    unittest.main(verbosity=2)

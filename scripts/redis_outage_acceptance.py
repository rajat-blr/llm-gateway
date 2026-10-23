"""Exercise a real Redis interruption while a gateway stream is in progress.

Requires the Compose Redis service on localhost:6381, built local binaries,
and examples/requirements.txt. The Redis service is restored in finally.
"""
import os
import subprocess

from openai import APIStatusError
from sdk_integration import MESSAGES, get_json, running

os.environ["REDIS_TEST_ADDR"] = "127.0.0.1:6381"
with running(("-chunks", "50", "-chunk-delay", "100ms")) as (client, _, stats):
    try:
        with client.chat.completions.create(
            model="default-chat", messages=MESSAGES, stream=True,
            stream_options={"include_usage": True},
        ) as stream:
            next(stream)
            subprocess.run(["docker", "compose", "stop", "redis"], check=True)
            try:
                client.chat.completions.create(model="default-chat", messages=MESSAGES)
            except APIStatusError as error:
                assert error.status_code == 503 and error.code == "dependency_unavailable"
            else:
                raise AssertionError("Redis outage admitted new inference")
            assert get_json(stats["primary"])["calls"] == 1
            chunks = list(stream)
        assert chunks[-1].usage.total_tokens == 55
        assert any(chunk.choices and chunk.choices[0].finish_reason == "stop" for chunk in chunks)
        print("Redis outage: new inference rejected; active stream completed")
    finally:
        subprocess.run(["docker", "compose", "up", "-d", "--wait", "redis"], check=True)
    result = client.chat.completions.create(model="default-chat", messages=MESSAGES)
    assert result.usage.total_tokens == 55
    print("Redis recovery: admission resumed without restarting the gateway")

"""Check the running Compose demo after selecting a documented fault scenario."""
import argparse
import json
import os
import urllib.request

parser = argparse.ArgumentParser()
parser.add_argument("scenario", choices=("normal", "fallback", "drop"), default="normal", nargs="?")
args = parser.parse_args()
base = os.environ.get("GATEWAY_BASE_URL", "http://localhost:8080/v1").removesuffix("/v1")
with urllib.request.urlopen(base + "/", timeout=5) as response:
    assert response.status == 200 and "text/html" in response.headers["Content-Type"]
    assert "request-form" in response.read().decode()
request = urllib.request.Request(
    base + "/v1/chat/completions",
    data=json.dumps({"model": "default-chat", "messages": [{"role": "user", "content": "Hello"}], "stream": True, "stream_options": {"include_usage": True}}).encode(),
    headers={"Content-Type": "application/json", "Authorization": "Bearer " + os.environ.get("GATEWAY_API_KEY", "demo-local-key")},
)
with urllib.request.urlopen(request, timeout=5) as response:
    assert response.status == 200
    provider = response.headers["X-Gateway-Provider"]
    assert response.headers["X-RateLimit-Remaining-Tokens"] is not None
    events = [line[6:] for line in response.read().decode().splitlines() if line.startswith("data: ")]
    payloads = [json.loads(event) for event in events if event != "[DONE]"]
    if args.scenario == "drop":
        assert provider == "mock-openai"
        assert "[DONE]" not in events
        assert any(payload.get("error", {}).get("code") == "upstream_failure" for payload in payloads)
    else:
        expected = "backup-anthropic" if args.scenario == "fallback" else "mock-openai"
        assert provider == expected, (provider, expected)
        assert events[-1] == "[DONE]"
        assert payloads[-1]["usage"]["total_tokens"] == 8
        text = "".join(choice.get("delta", {}).get("content", "") for payload in payloads for choice in payload.get("choices", []))
        assert text == "hello hello hello "
print(f"Compose {args.scenario}: passed (provider={provider})")

#!/usr/bin/env python3
"""Validate the local monitoring stack through its public HTTP APIs."""
import json
import os
import time
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)


def main():
    request = urllib.request.Request(
        "http://localhost:8080/v1/chat/completions",
        data=json.dumps({"model": "default-chat", "messages": [{"role": "user", "content": "monitoring acceptance"}], "stream": True}).encode(),
        headers={"Authorization": "Bearer " + os.getenv("GATEWAY_API_KEY", "demo-local-key"), "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        assert response.status == 200
        assert response.read().endswith(b"data: [DONE]\n\n")
    deadline = time.monotonic() + 30
    while True:
        targets = get("http://localhost:9096/api/v1/targets")["data"]["activeTargets"]
        samples = get("http://localhost:9096/api/v1/query?query=llmgw_first_text_seconds_count")["data"]["result"]
        if targets and all(target["health"] == "up" for target in targets) and samples:
            break
        if time.monotonic() >= deadline:
            raise AssertionError((targets, samples))
        time.sleep(1)
    assert get("http://localhost:3000/api/health")["database"] == "ok"
    dashboard = get("http://localhost:3000/api/dashboards/uid/llm-gateway")["dashboard"]
    assert dashboard["title"] == "LLM Gateway"
    assert len(dashboard["panels"]) >= 12
    assert get("http://localhost:3000/api/datasources/uid/gateway-prometheus")["url"] == "http://prometheus:9090"
    # Resolve every panel expression through Prometheus, including empty series.
    # This catches provisioned-but-broken PromQL without browser automation.
    from urllib.parse import urlencode
    for panel in dashboard["panels"]:
        for target in panel["targets"]:
            result = get("http://localhost:9096/api/v1/query?" + urlencode({"query": target["expr"]}))
            assert result["status"] == "success", target
    print("Monitoring acceptance passed: scrape up, first-text metric, Grafana provisioned, all panel queries valid")


if __name__ == "__main__":
    main()

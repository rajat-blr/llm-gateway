#!/usr/bin/env python3
"""Closed-loop local fixture baseline, with no paid traffic or capacity claims.

Run using the SDK test environment after make build. Results include every run,
client/gateway/mock CPU, sampled gateway runtime resources and usage loss.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor
import http.client
import hashlib
import socket
import json
import math
import os
from pathlib import Path
import platform
import resource
import subprocess
import threading
import time
import urllib.request
from urllib.parse import urlsplit

from sdk_integration import ROOT, get_json, running


def metrics(url):
    values = {}
    with urllib.request.urlopen(url + "/metrics", timeout=3) as response:
        for line in response.read().decode().splitlines():
            if not line or line.startswith("#"):
                continue
            key, raw = line.rsplit(" ", 1)
            name = key.split("{", 1)[0]
            value = float(raw)
            if math.isfinite(value):
                values[name] = values.get(name, 0) + value
    return values


def process_stats(pid):
    raw = subprocess.check_output(["ps", "-p", str(pid), "-o", "time=,rss="], text=True).split()
    parts = raw[0].split(":")
    seconds = float(parts[-1]) + int(parts[-2]) * 60
    if len(parts) == 3:
        seconds += int(parts[0]) * 3600
    return {"cpu_seconds": seconds, "rss_bytes": int(raw[1]) * 1024}


def percentile(values, fraction):
    if not values:
        return None
    values = sorted(values)
    return values[max(0, math.ceil(len(values) * fraction) - 1)]


def summarize(values):
    return {"samples": len(values), "p50_ms": percentile(values, .5), "p95_ms": percentile(values, .95), "p99_ms": percentile(values, .99)}


def one_request(connection, payload, cancel=False, expected_text="hello " * 3, api_key="sdk-test-key"):
    start = time.perf_counter()
    connection.request("POST", "/v1/chat/completions", payload, {"Content-Type": "application/json", "Authorization": "Bearer " + api_key})
    response = connection.getresponse()
    if response.status != 200:
        response.read()
        raise RuntimeError("HTTP " + str(response.status))
    first_text = None
    if '"stream": true' in payload:
        done = False
        text = ""
        finish = False
        while True:
            line = response.readline()
            if not line:
                break
            if not line.startswith(b"data: "):
                continue
            data = line[6:].strip()
            if data == b"[DONE]":
                done = True
                break
            event = json.loads(data)
            if "error" in event:
                raise RuntimeError("stream error")
            for choice in event["choices"]:
                delta = choice.get("delta", {}).get("content", "")
                text += delta
                if choice.get("finish_reason") == "stop":
                    finish = True
                if delta and first_text is None:
                    first_text = (time.perf_counter() - start) * 1000
                    if cancel:
                        # Closing HTTPResponse alone may drain/reuse the socket.
                        # Close the underlying connection to exercise disconnect.
                        connection.close()
                        response.close()
                        return None, first_text
        response.read()
        response.close()
        if not done or not finish or text != expected_text:
            raise RuntimeError("incomplete or incorrect stream")
    else:
        value = json.loads(response.read())
        response.close()
        if value["choices"][0]["message"]["content"] != expected_text:
            raise RuntimeError("incorrect completion")
    return (time.perf_counter() - start) * 1000, first_text


def stage(url, payload, concurrency, seconds, management, gateway_pid, mock_pid, expected_text="hello " * 3, api_key="sdk-test-key", unique=False):
    target = urlsplit(url)
    # Warm each connection before measurement. Warmups are not counted.
    barrier = threading.Barrier(concurrency + 1)
    latencies, first_texts, errors, peaks = [], [], [], {}
    lock = threading.Lock()
    start_signal = threading.Event()
    stop_sampler = threading.Event()
    end = [0.0]

    sequence = 0

    def worker():
        nonlocal sequence
        with lock:
            worker_id = sequence
            sequence += 1
        count = 0
        unique_prefix = str(time.time_ns()) + "-" + str(worker_id)
        def request_body():
            nonlocal count
            if not unique:
                return payload
            count += 1
            value = json.loads(payload)
            value["messages"][-1]["content"] += f" {unique_prefix}-{count}"
            return json.dumps(value)
        connection = http.client.HTTPConnection(target.hostname, target.port, timeout=5)
        try:
            one_request(connection, request_body(), expected_text=expected_text, api_key=api_key)
        except Exception as error:
            with lock:
                errors.append("warmup: " + str(error))
        barrier.wait(timeout=30)
        start_signal.wait()
        local_latencies, local_firsts, local_errors = [], [], []
        try:
            while time.perf_counter() < end[0]:
                try:
                    latency, first = one_request(connection, request_body(), expected_text=expected_text, api_key=api_key)
                    local_latencies.append(latency)
                    if first is not None:
                        local_firsts.append(first)
                except Exception as error:
                    local_errors.append(str(error))
                    connection.close()
        finally:
            connection.close()
        with lock:
            latencies.extend(local_latencies)
            first_texts.extend(local_firsts)
            errors.extend(local_errors)

    def sample():
        while not stop_sampler.wait(.25):
            try:
                for name, value in metrics(management).items():
                    if name in ("go_goroutines", "go_memstats_heap_alloc_bytes", "go_memstats_heap_inuse_bytes", "llmgw_active_attempts", "llmgw_active_streams", "llmgw_usage_queue_depth"):
                        peaks[name] = max(value, peaks.get(name, 0))
            except Exception as error:
                with lock:
                    errors.append("sampler: " + str(error))

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker) for _ in range(concurrency)]
        barrier.wait(timeout=30)
        before = metrics(management)
        cpu_before = {"gateway": process_stats(gateway_pid), "mock": process_stats(mock_pid)}
        generator_before = resource.getrusage(resource.RUSAGE_SELF)
        start = time.perf_counter()
        end[0] = start + seconds
        sampler = threading.Thread(target=sample)
        sampler.start()
        start_signal.set()
        try:
            for future in futures:
                future.result()
            elapsed = time.perf_counter() - start
            generator_after = resource.getrusage(resource.RUSAGE_SELF)
        finally:
            end[0] = 0
            stop_sampler.set()
            sampler.join()
    cpu_after = {"gateway": process_stats(gateway_pid), "mock": process_stats(mock_pid)}
    # Keep durable write backlog out of the next stage; record any losses.
    deadline = time.monotonic() + 10
    while True:
        after = metrics(management)
        if after.get("llmgw_usage_queue_depth", 0) == 0 and after.get("llmgw_usage_in_flight", 0) == 0:
            break
        if time.monotonic() >= deadline:
            errors.append("usage queue did not drain within 10s")
            break
        time.sleep(.05)
    cpu = {name: {"cpu_seconds": cpu_after[name]["cpu_seconds"] - cpu_before[name]["cpu_seconds"], "mean_cpu_cores": (cpu_after[name]["cpu_seconds"] - cpu_before[name]["cpu_seconds"]) / elapsed, "rss_after_bytes": cpu_after[name]["rss_bytes"]} for name in cpu_after}
    generator_cpu = generator_after.ru_utime + generator_after.ru_stime - generator_before.ru_utime - generator_before.ru_stime
    return {"duration_including_inflight_drain_seconds": elapsed, "successful_requests": len(latencies), "observed_requests_per_second": len(latencies) / elapsed, "errors": len(errors), "warmup_errors": sum(error.startswith("warmup:") for error in errors), "measurement_errors": sum(not error.startswith("warmup:") for error in errors), "error_examples": errors[:5], "latency": summarize(latencies), "first_text": summarize(first_texts), "cpu": cpu, "generator_mean_cpu_cores": generator_cpu / elapsed, "generator_process_peak_rss_bytes": generator_after.ru_maxrss * (1 if platform.system() == "Darwin" else 1024), "sampled_peaks": peaks, "counter_deltas": {name: after.get(name, 0) - before.get(name, 0) for name in ("llmgw_requests_total", "llmgw_attempts_total", "llmgw_usage_dropped_total", "llmgw_usage_write_failures_total", "llmgw_accounting_errors_total", "llmgw_cache_hits_total", "llmgw_cache_misses_total", "llmgw_cache_coalesced_total", "llmgw_fallbacks_total", "llmgw_breaker_skips_total")}}


def soak(client, stats, rounds, concurrency):
    target = urlsplit(str(client.base_url))
    management = stats["management"]
    payload = json.dumps({"model": "default-chat", "messages": [{"role": "user", "content": "soak"}], "stream": True, "max_tokens": 16})
    snapshots = []
    canceled_before = get_json(stats["primary"])["canceled"]
    for round_index in range(rounds):
        def request(index):
            connection = http.client.HTTPConnection(target.hostname, target.port, timeout=5)
            try:
                one_request(connection, payload, cancel=index % 2 == 0)
            finally:
                connection.close()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            list(pool.map(request, range(concurrency)))
        deadline = time.monotonic() + 5
        while True:
            sample = metrics(management)
            if get_json(stats["primary"])["canceled"] - canceled_before == (round_index + 1) * ((concurrency + 1) // 2) and all(sample.get(name, 0) == 0 for name in ("llmgw_active_attempts", "llmgw_active_streams", "llmgw_active_work_slots", "llmgw_shared_executions", "llmgw_shared_waiters", "llmgw_usage_queue_depth", "llmgw_usage_in_flight")):
                break
            if time.monotonic() >= deadline:
                raise AssertionError("resources did not drain: " + str(sample))
            time.sleep(.025)
        snapshots.append({"round": round_index + 1, **{name: sample.get(name, 0) for name in ("go_goroutines", "go_memstats_heap_alloc_bytes", "go_memstats_heap_inuse_bytes", "llmgw_usage_dropped_total", "llmgw_accounting_errors_total", "llmgw_cache_hits_total", "llmgw_cache_misses_total", "llmgw_cache_coalesced_total", "llmgw_fallbacks_total", "llmgw_breaker_skips_total")}})
    return {"rounds": rounds, "concurrency": concurrency, "disconnect_fraction": ((concurrency + 1) // 2) / concurrency, "upstream_cancellations": get_json(stats["primary"])["canceled"] - canceled_before, "quiescent_samples": snapshots, "checks": "Every disconnect canceled upstream; every round drained active streams/attempts/slots/shared jobs/waiters/usage within 5s; heap/goroutines reported without a leak-free claim."}


def machine_details():
    details = {}
    if platform.system() == "Darwin":
        for key in ("machdep.cpu.brand_string", "hw.memsize"):
            details[key] = subprocess.check_output(["sysctl", "-n", key], text=True).strip()
    elif platform.system() == "Linux":
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                details["cpu_model"] = line.split(":", 1)[1].strip()
                break
        for line in Path("/proc/meminfo").read_text().splitlines():
            if line.startswith("MemTotal:"):
                details["memory_bytes"] = int(line.split()[1]) * 1024
    return details


def redis_version(address):
    # INFO is bounded and read-only; no keyspace/credentials are recorded.
    host, port = address.rsplit(":", 1)
    with socket.create_connection((host, int(port)), timeout=2) as connection:
        connection.sendall(b"*2\r\n$4\r\nINFO\r\n$6\r\nserver\r\n")
        with connection.makefile("rb") as response:
            header = response.readline(100)
            if not header.startswith(b"$"):
                return "unavailable"
            size = int(header[1:].strip())
            if size < 0 or size > 65536:
                raise ValueError("unexpected Redis INFO size")
            for line in response.read(size).decode().splitlines():
                if line.startswith("redis_version:"):
                    return line.split(":", 1)[1]
    return "unavailable"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duration", type=float, default=2)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 10, 100])
    parser.add_argument("--soak-rounds", type=int, default=10)
    parser.add_argument("--soak-concurrency", type=int, default=32)
    parser.add_argument("--output", type=Path, default=ROOT / "tmp/baseline.json")
    args = parser.parse_args()
    if not (0 < args.duration <= 30 and 1 <= args.runs <= 10 and 1 <= args.soak_rounds <= 100 and 1 <= args.soak_concurrency <= 500 and all(1 <= n <= 500 for n in args.concurrency)):
        parser.error("duration <=30s, runs <=10, concurrency <=500, soak rounds <=100 required")
    overrides = {"token_limit": {"capacity": 1_000_000_000, "refill_per_second": 1_000_000}, "max_concurrent": 512, "max_streams": 512}
    results = {"schema": 1, "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "gateway_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(), "environment": {"os": platform.platform(), "architecture": platform.machine(), "logical_cpus": os.cpu_count(), "go": subprocess.check_output(["go", "version"], text=True).strip(), "python": platform.python_version()}, "method": {"model": "default-chat", "fixture": {"first_delay_ms": 20, "chunk_delay_ms": 30, "chunks": 3, "payload": "hello ", "fail_every": 0}, "max_tokens": 16, "cache": "BYPASS (bootstrap key reuse disabled)", "admission": overrides, "warmup": "one request per worker before each stage", "measurement": "closed loop persistent HTTP connection per worker; nearest-rank percentiles; alternating direct/gateway stage order each run; 250ms runtime sampling", "limits": "Local fixture evidence, not an open-loop capacity test. Python generator, fixture, gateway and Docker Redis share a host. End-to-end percentiles must not be subtracted as per-request overhead."}, "stages": []}
    with running(overrides=overrides) as (client, gateway, stats):
        # Read only our fixture subprocess PID via its owned process tree.
        # running exposes process IDs explicitly to avoid unrelated processes.
        config = json.loads(Path(stats["config"]).read_text())
        results["environment"]["hardware"] = machine_details()
        results["environment"]["redis_version"] = redis_version(config["redis"]["address"])
        results["method"]["driver_sha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
        results["method"]["duration_seconds_per_stage"] = args.duration
        results["method"]["runs"] = args.runs
        results["method"]["concurrency"] = args.concurrency
        results["environment"]["gateway_binary_sha256"] = hashlib.sha256((ROOT / "bin/gateway").read_bytes()).hexdigest()
        results["environment"]["gateway_source_dirty"] = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", "cmd", "internal", "go.mod", "go.sum"], cwd=ROOT).returncode != 0
        results["method"]["sqlite"] = {key: value for key, value in config["sqlite"].items() if key != "path"}
        results["method"]["bounds"] = {key: config[key] for key in ("request_timeout", "idle_timeout", "write_timeout", "shutdown_timeout", "max_body_bytes", "max_output_bytes", "max_event_bytes")}
        mock_pid = stats["primary_pid"]
        direct_url = stats["primary"].removesuffix("/stats")
        for concurrency in args.concurrency:
            for mode in ("json", "stream"):
                payload = json.dumps({"model": "default-chat", "messages": [{"role": "user", "content": "baseline"}], "stream": mode == "stream", "max_tokens": 16})
                for run in range(args.runs):
                    targets = [("direct", direct_url), ("gateway", str(client.base_url))]
                    if run % 2:
                        targets.reverse()
                    for name, url in targets:
                        result = stage(url, payload, concurrency, args.duration, stats["management"], gateway.pid, mock_pid)
                        result.update(target=name, mode=mode, concurrency=concurrency, run=run + 1)
                        results["stages"].append(result)
                        print(f"{name} {mode} c={concurrency} run={run+1}: {result['observed_requests_per_second']:.1f} req/s, errors={result['errors']}", flush=True)
        results["soak"] = soak(client, stats, args.soak_rounds, args.soak_concurrency)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(results, indent=2) + "\n")
    if any(stage["errors"] for stage in results["stages"]):
        raise SystemExit("Baseline recorded errors; inspect " + str(args.output))
    print("Baseline and bounded cancellation soak recorded in", args.output)


if __name__ == "__main__":
    main()

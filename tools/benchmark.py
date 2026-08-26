#!/usr/bin/env python3
"""End-to-end MQTT throughput benchmark for Pipistrelle.

Each client subscribes to a private topic and publishes to itself, so a completed
message has crossed CONNECT/SUBSCRIBE/PUBLISH routing and the network stack.
"""
from __future__ import annotations

import argparse
import json
import os
import statistics
import threading
import time
from dataclasses import dataclass, asdict

import paho.mqtt.client as mqtt


@dataclass
class Result:
    clients: int
    messages_per_client: int
    total_messages: int
    qos: int
    payload_bytes: int
    elapsed_seconds: float
    messages_per_second: float
    mebibytes_per_second: float
    connect_p50_ms: float
    connect_p95_ms: float
    failures: int


class Worker:
    def __init__(self, idx: int, args: argparse.Namespace, start_event: threading.Event):
        self.idx = idx
        self.args = args
        self.start_event = start_event
        self.ready = threading.Event()
        self.done = threading.Event()
        self.failed = None
        self.received = 0
        self.connect_ms = 0.0
        self.topic = f"bench/{idx}"
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            protocol=mqtt.MQTTv5,
            client_id=f"pipistrelle_bench_{idx}",
            transport=args.transport,
        )
        self.client.username_pw_set(args.username, args.password)
        self.client.max_inflight_messages_set(max(100, args.messages))
        if args.tls:
            self.client.tls_set(ca_certs=args.ca if args.ca else None)
            if args.tls_insecure:
                self.client.tls_insecure_set(True)
        self.client.on_connect = self._on_connect
        self.client.on_subscribe = self._on_subscribe
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        if int(reason_code.value) != 0:
            self.failed = f"connect reason={reason_code}"
            self.done.set()
            return
        client.subscribe(self.topic, qos=self.args.qos)

    def _on_subscribe(self, client, userdata, mid, reason_codes, properties):
        if not reason_codes or int(reason_codes[0].value) > 2:
            self.failed = f"subscribe reasons={reason_codes}"
            self.done.set()
            return
        self.ready.set()

    def _on_message(self, client, userdata, msg):
        self.received += 1
        if self.received >= self.args.messages:
            self.done.set()

    def run(self):
        try:
            t0 = time.perf_counter()
            self.client.connect(self.args.host, self.args.port, keepalive=60)
            self.client.loop_start()
            if not self.ready.wait(self.args.timeout):
                self.failed = self.failed or "ready timeout"
                self.done.set()
                return
            self.connect_ms = (time.perf_counter() - t0) * 1000.0
            if not self.start_event.wait(self.args.timeout):
                self.failed = "start timeout"
                self.done.set()
                return
            payload = b"x" * self.args.payload
            for _ in range(self.args.messages):
                info = self.client.publish(self.topic, payload, qos=self.args.qos)
                if info.rc != mqtt.MQTT_ERR_SUCCESS:
                    self.failed = f"publish rc={info.rc}"
                    self.done.set()
                    break
        except Exception as exc:
            self.failed = repr(exc)
            self.done.set()

    def close(self):
        try:
            self.client.disconnect()
            self.client.loop_stop()
        except Exception:
            pass


def percentile(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    pos = (len(values) - 1) * p
    lo = int(pos)
    hi = min(lo + 1, len(values) - 1)
    frac = pos - lo
    return values[lo] * (1.0 - frac) + values[hi] * frac


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--clients", type=int, default=10)
    ap.add_argument("--messages", type=int, default=1000)
    ap.add_argument("--payload", type=int, default=128, help="payload size in bytes")
    ap.add_argument("--qos", type=int, choices=(0, 1), default=0)
    ap.add_argument("--username", default=os.getenv("PIPISTRELLE_BENCH_USER", "admin"))
    ap.add_argument("--password", default=os.getenv("PIPISTRELLE_BENCH_PASSWORD", "admin123"))
    ap.add_argument("--transport", choices=("tcp", "websockets"), default="tcp")
    ap.add_argument("--tls", action="store_true")
    ap.add_argument("--tls-insecure", action="store_true", help="accept self-signed test certificate")
    ap.add_argument("--ca")
    ap.add_argument("--timeout", type=float, default=30.0)
    ap.add_argument("--json-out")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    if args.clients < 1 or args.messages < 1 or args.payload < 0:
        raise SystemExit("clients/messages must be positive and payload non-negative")

    start_event = threading.Event()
    workers = [Worker(i, args, start_event) for i in range(args.clients)]
    threads = [threading.Thread(target=w.run, daemon=True) for w in workers]
    for t in threads:
        t.start()

    deadline = time.monotonic() + args.timeout
    for w in workers:
        remaining = max(0.0, deadline - time.monotonic())
        if not w.ready.wait(remaining):
            w.failed = w.failed or "setup timeout"

    setup_failures = [w for w in workers if w.failed]
    if setup_failures:
        for w in workers:
            w.close()
        print(json.dumps({"status": "failed", "failures": [w.failed for w in setup_failures]}, indent=2))
        return 2

    start = time.perf_counter()
    start_event.set()
    deadline = time.monotonic() + args.timeout
    for w in workers:
        remaining = max(0.0, deadline - time.monotonic())
        if not w.done.wait(remaining):
            w.failed = w.failed or f"receive timeout ({w.received}/{args.messages})"
    elapsed = time.perf_counter() - start

    total_received = sum(w.received for w in workers)
    failures = sum(1 for w in workers if w.failed)
    connect_times = [w.connect_ms for w in workers if not w.failed]
    result = Result(
        clients=args.clients,
        messages_per_client=args.messages,
        total_messages=total_received,
        qos=args.qos,
        payload_bytes=args.payload,
        elapsed_seconds=elapsed,
        messages_per_second=(total_received / elapsed) if elapsed else 0.0,
        mebibytes_per_second=((total_received * args.payload) / 1048576.0 / elapsed) if elapsed else 0.0,
        connect_p50_ms=statistics.median(connect_times) if connect_times else 0.0,
        connect_p95_ms=percentile(connect_times, 0.95),
        failures=failures,
    )
    output = {"status": "ok" if failures == 0 else "partial", **asdict(result)}
    print(json.dumps(output, indent=2))
    if args.json_out:
        out = Path(args.json_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(output, indent=2) + "\n")

    for w in workers:
        w.close()
    return 0 if failures == 0 else 3


if __name__ == "__main__":
    from pathlib import Path
    raise SystemExit(main())

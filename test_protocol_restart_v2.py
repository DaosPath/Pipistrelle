#!/usr/bin/env python3
"""Destructive MQTT v5 restart/compliance tests for the local Orange Pi test container.

This suite intentionally SIGKILLs `pipistrelle_broker_test`. Run only against the
local development compose instance, never against a production broker.
"""
import subprocess
import time
import urllib.request

from test_protocol_v2 import (
    RawClient,
    ack_packet,
    no_packet,
    parse_publish,
    publish_packet,
)

CONTAINER = "pipistrelle_broker_test"
HEALTH = "http://127.0.0.1:9095/health"


def docker(*args):
    subprocess.run(["docker", *args], check=True, stdout=subprocess.DEVNULL)


def wait_health(timeout=15.0):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(HEALTH, timeout=0.5) as response:
                if response.status == 200:
                    return
        except Exception as exc:
            last = exc
        time.sleep(0.15)
    raise AssertionError(f"broker did not become healthy: {last}")


def hard_restart():
    docker("kill", "--signal=KILL", CONTAINER)
    time.sleep(0.35)
    docker("start", CONTAINER)
    wait_health()


def clear_retained(topic, client_id):
    cleaner = RawClient(client_id)
    cleaner.publish(topic, b"", retain=True)
    cleaner.disconnect()


def test_active_will_survives_sigkill():
    topic = "protocol/v2/restart/active-will"
    clear_retained(topic, "restart_active_clean")
    props = {
        "payload_format_indicator": 1,
        "message_expiry_interval": 12,
        "content_type": "text/plain",
        "response_topic": "protocol/v2/restart/active-will/reply",
        "correlation_data": b"active-crash-corr",
        "user_properties": [("crash", "active"), ("order", "two")],
    }
    active = RawClient(
        "restart_active_will_owner",
        clean_start=True,
        session_expiry=30,
        will={
            "topic": topic,
            "payload": b"active-crash-will",
            "qos": 1,
            "retain": True,
            "delay": 0,
            "properties": props,
        },
    )
    hard_restart()  # active socket is deliberately destroyed without DISCONNECT
    try:
        active.sock.close()
    except Exception:
        pass

    sub = RawClient("restart_active_will_check")
    sub.subscribe(topic, qos=1, subscription_id=111)
    h, body = sub.expect_type(3, timeout=5)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["retain"] and msg["payload"] == b"active-crash-will", msg
    assert got["payload_format_indicator"] == 1, got
    assert got["content_type"] == "text/plain", got
    assert got["response_topic"] == "protocol/v2/restart/active-will/reply", got
    assert got["correlation_data"] == b"active-crash-corr", got
    assert got["user_properties"] == [("crash", "active"), ("order", "two")], got
    assert got["subscription_identifiers"] == [111], got
    assert 1 <= got["message_expiry_interval"] <= 12, got
    sub.sock.sendall(ack_packet(4, msg["pid"]))
    sub.disconnect()


def test_delayed_will_survives_restart_and_keeps_deadline():
    topic = "protocol/v2/restart/delayed-will"
    clear_retained(topic, "restart_delay_clean")
    owner = RawClient(
        "restart_delayed_will_owner",
        clean_start=True,
        session_expiry=30,
        will={
            "topic": topic,
            "payload": b"delayed-after-crash",
            "qos": 1,
            "retain": True,
            "delay": 8,
            "properties": {
                "message_expiry_interval": 10,
                "content_type": "text/plain",
                "correlation_data": b"delay-corr",
                "user_properties": [("delay", "kept")],
            },
        },
    )
    owner.close_abrupt()
    time.sleep(1.0)
    hard_restart()

    sub = RawClient("restart_delay_check")
    sub.subscribe(topic, qos=1)
    assert no_packet(sub, 2.0), "persisted Will Delay became immediate after restart"
    h, body = sub.expect_type(3, timeout=7)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["payload"] == b"delayed-after-crash", msg
    assert got["content_type"] == "text/plain" and got["correlation_data"] == b"delay-corr", got
    assert got["user_properties"] == [("delay", "kept")], got
    # Will Message Expiry starts at Will publication, not while Will Delay is counting.
    assert 8 <= got["message_expiry_interval"] <= 10, got
    sub.sock.sendall(ack_packet(4, msg["pid"]))
    sub.disconnect()


def test_delayed_will_can_be_cancelled_after_restart():
    topic = "protocol/v2/restart/cancel-will"
    clear_retained(topic, "restart_cancel_clean")
    owner = RawClient(
        "restart_cancel_will_owner",
        clean_start=True,
        session_expiry=30,
        will={
            "topic": topic,
            "payload": b"must-not-publish",
            "qos": 0,
            "retain": True,
            "delay": 7,
            "properties": {"content_type": "text/plain", "user_properties": [("cancel", "yes")]},
        },
    )
    owner.close_abrupt()
    time.sleep(1.0)
    hard_restart()

    resumed = RawClient("restart_cancel_will_owner", clean_start=False, session_expiry=30)
    assert resumed.session_present
    resumed.disconnect()
    time.sleep(7.0)

    check = RawClient("restart_cancel_will_check")
    check.subscribe(topic, qos=0)
    assert no_packet(check, 0.8), "cancelled persisted Will was published"
    check.disconnect()


def test_takeover_suppressed_delayed_will_stays_deleted_after_restart():
    topic = "protocol/v2/restart/takeover-suppressed-will"
    clear_retained(topic, "restart_takeover_will_clean")
    cid = "restart_takeover_will_owner"
    old = RawClient(
        cid,
        clean_start=True,
        session_expiry=30,
        will={
            "topic": topic,
            "payload": b"stale-will-must-not-resurrect",
            "qos": 1,
            "retain": True,
            "delay": 8,
            "properties": {
                "content_type": "text/plain",
                "user_properties": [("takeover", "suppressed")],
            },
        },
    )
    # Continuing the same Session before Will Delay expires suppresses old Will.
    replacement = RawClient(cid, clean_start=False, session_expiry=30)
    assert replacement.session_present
    _, body = old.expect_type(14, timeout=3)
    assert body and body[0] == 0x8E, body
    try:
        old.sock.close()
    except Exception:
        pass
    # Crash after takeover. A stale persisted Will row must not be resurrected.
    hard_restart()
    try:
        replacement.sock.close()
    except Exception:
        pass
    time.sleep(8.5)
    check = RawClient("restart_takeover_will_check")
    check.subscribe(topic, qos=1)
    assert no_packet(check, 0.8), "suppressed takeover Will resurrected after restart"
    check.disconnect()


def test_retained_properties_survive_restart():
    topic = "protocol/v2/restart/retained-properties"
    clear_retained(topic, "restart_ret_props_clean")
    pub = RawClient("restart_ret_props_pub")
    pub.publish(
        topic,
        b"retained-across-restart",
        qos=1,
        retain=True,
        pid=231,
        properties={
            "message_expiry_interval": 20,
            "content_type": "application/json",
            "correlation_data": b"ret-restart-corr",
            "user_properties": [("persist", "retained"), ("order", "two")],
        },
    )
    pub.disconnect()
    time.sleep(0.5)
    hard_restart()

    sub = RawClient("restart_ret_props_sub")
    sub.subscribe(topic, qos=1, subscription_id=121)
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["retain"] and msg["payload"] == b"retained-across-restart", msg
    assert got["content_type"] == "application/json", got
    assert got["correlation_data"] == b"ret-restart-corr", got
    assert got["user_properties"] == [("persist", "retained"), ("order", "two")], got
    assert got["subscription_identifiers"] == [121], got
    assert 1 <= got["message_expiry_interval"] < 20, got
    sub.sock.sendall(ack_packet(4, msg["pid"]))
    sub.disconnect()


def test_offline_qos1_properties_survive_restart():
    topic = "protocol/v2/restart/offline-qos1-properties"
    cid = "restart_offline_qos1_sub"
    sub = RawClient(cid, clean_start=True, session_expiry=60)
    sub.subscribe(topic, qos=1, subscription_id=131)
    sub.disconnect()
    time.sleep(0.1)

    pub = RawClient("restart_offline_qos1_pub")
    pub.publish(
        topic,
        b"offline-qos1-restart",
        qos=1,
        pid=232,
        properties={
            "message_expiry_interval": 20,
            "content_type": "application/octet-stream",
            "correlation_data": b"q1-restart-corr",
            "user_properties": [("persist", "qos1")],
        },
    )
    pub.disconnect()
    time.sleep(0.5)
    hard_restart()

    resumed = RawClient(cid, clean_start=False, session_expiry=60)
    assert resumed.session_present
    h, body = resumed.expect_type(3)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["dup"] and msg["payload"] == b"offline-qos1-restart", msg
    assert got["content_type"] == "application/octet-stream", got
    assert got["correlation_data"] == b"q1-restart-corr", got
    assert got["user_properties"] == [("persist", "qos1")], got
    assert got["subscription_identifiers"] == [131], got
    assert 1 <= got["message_expiry_interval"] < 20, got
    resumed.sock.sendall(ack_packet(4, msg["pid"]))
    resumed.disconnect()


def test_inbound_qos2_properties_survive_pubrec_restart():
    topic = "protocol/v2/restart/inbound-qos2-properties"
    sub_id = "restart_in_qos2_sub"
    sub = RawClient(sub_id, clean_start=True, session_expiry=60)
    sub.subscribe(topic, qos=2, subscription_id=141)
    sub.disconnect()

    pub_id = "restart_in_qos2_pub"
    pub = RawClient(pub_id, clean_start=True, session_expiry=60)
    pub.sock.sendall(
        publish_packet(
            topic,
            b"qos2-properties-restart",
            qos=2,
            pid=233,
            properties={
                "message_expiry_interval": 20,
                "content_type": "application/cbor",
                "correlation_data": b"q2-in-corr",
                "user_properties": [("persist", "qos2-in")],
            },
        )
    )
    pub.expect_type(5)
    # Ownership is at the broker (PUBREC sent), but Application Message is not routed until PUBREL.
    hard_restart()
    try:
        pub.sock.close()
    except Exception:
        pass

    resumed_pub = RawClient(pub_id, clean_start=False, session_expiry=60)
    assert resumed_pub.session_present
    resumed_pub.sock.sendall(ack_packet(6, 233))
    resumed_pub.expect_type(7)

    resumed_sub = RawClient(sub_id, clean_start=False, session_expiry=60)
    assert resumed_sub.session_present
    h, body = resumed_sub.expect_type(3)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["qos"] == 2 and msg["payload"] == b"qos2-properties-restart", msg
    assert got["content_type"] == "application/cbor", got
    assert got["correlation_data"] == b"q2-in-corr", got
    assert got["user_properties"] == [("persist", "qos2-in")], got
    assert got["subscription_identifiers"] == [141], got
    resumed_sub.sock.sendall(ack_packet(5, msg["pid"]))
    _, rel = resumed_sub.expect_type(6)
    resumed_sub.sock.sendall(ack_packet(7, msg["pid"]))
    resumed_sub.disconnect(); resumed_pub.disconnect()


def test_outbound_qos2_started_properties_survive_restart():
    topic = "protocol/v2/restart/outbound-qos2-properties"
    cid = "restart_out_qos2_sub"
    sub = RawClient(cid, clean_start=True, session_expiry=60)
    sub.subscribe(topic, qos=2, subscription_id=151)
    pub = RawClient("restart_out_qos2_pub")
    pub.sock.sendall(
        publish_packet(
            topic,
            b"out-qos2-properties",
            qos=2,
            pid=234,
            properties={
                "message_expiry_interval": 20,
                "content_type": "application/protobuf",
                "correlation_data": b"q2-out-corr",
                "user_properties": [("persist", "qos2-out")],
            },
        )
    )
    pub.expect_type(5)
    pub.sock.sendall(ack_packet(6, 234))
    pub.expect_type(7)
    h, body = sub.expect_type(3)
    first = parse_publish(h, body)
    assert first["qos"] == 2
    # Lose the network before PUBREC: this PUBLISH has started and must be retried after restart.
    hard_restart()
    try:
        sub.sock.close(); pub.sock.close()
    except Exception:
        pass

    resumed = RawClient(cid, clean_start=False, session_expiry=60)
    assert resumed.session_present
    h, body = resumed.expect_type(3)
    retry = parse_publish(h, body)
    got = retry["parsed_properties"]
    assert retry["dup"] and retry["pid"] == first["pid"], retry
    assert retry["payload"] == b"out-qos2-properties", retry
    assert got["content_type"] == "application/protobuf", got
    assert got["correlation_data"] == b"q2-out-corr", got
    assert got["user_properties"] == [("persist", "qos2-out")], got
    assert got["subscription_identifiers"] == [151], got
    resumed.sock.sendall(ack_packet(5, retry["pid"]))
    _, rel = resumed.expect_type(6)
    resumed.sock.sendall(ack_packet(7, retry["pid"]))
    resumed.disconnect()


def run(name, fn):
    started = time.time()
    fn()
    print(f"[PASS] {name} ({time.time() - started:.3f}s)")


if __name__ == "__main__":
    wait_health()
    tests = [
        ("active Will survives SIGKILL", test_active_will_survives_sigkill),
        ("delayed Will deadline survives restart", test_delayed_will_survives_restart_and_keeps_deadline),
        ("persisted delayed Will cancels after restart", test_delayed_will_can_be_cancelled_after_restart),
        ("suppressed takeover Will stays deleted after restart", test_takeover_suppressed_delayed_will_stays_deleted_after_restart),
        ("retained properties survive restart", test_retained_properties_survive_restart),
        ("offline QoS1 properties survive restart", test_offline_qos1_properties_survive_restart),
        ("inbound QoS2 properties survive PUBREC restart", test_inbound_qos2_properties_survive_pubrec_restart),
        ("outbound QoS2 properties survive restart", test_outbound_qos2_started_properties_survive_restart),
    ]
    for name, fn in tests:
        run(name, fn)
    print(f"ALL {len(tests)} destructive restart/compliance tests PASSED")

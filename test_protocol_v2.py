#!/usr/bin/env python3
import socket
import struct
import time

HOST = "127.0.0.1"
PORT = 1883
USER = "admin"
PASSWORD = "admin123"


def varint(value: int) -> bytes:
    out = bytearray()
    while True:
        byte = value % 128
        value //= 128
        if value:
            byte |= 0x80
        out.append(byte)
        if not value:
            return bytes(out)


def read_varint_bytes(data: bytes, offset=0):
    value = 0
    mult = 1
    for i in range(4):
        if offset + i >= len(data):
            raise ValueError("incomplete varint")
        b = data[offset + i]
        value += (b & 0x7F) * mult
        if b & 0x80 == 0:
            return value, i + 1
        mult *= 128
    raise ValueError("malformed varint")


def utf8(value: str) -> bytes:
    raw = value.encode()
    return struct.pack("!H", len(raw)) + raw


def binary(value: bytes) -> bytes:
    return struct.pack("!H", len(value)) + value


def packet(header: int, body: bytes) -> bytes:
    return bytes([header]) + varint(len(body)) + body


def connect_packet(client_id, *, clean_start=True, session_expiry=0, will=None, user=USER, password=PASSWORD):
    flags = 0x80 | 0x40
    if clean_start:
        flags |= 0x02
    if will is not None:
        flags |= 0x04 | ((will.get("qos", 0) & 0x03) << 3)
        if will.get("retain", False):
            flags |= 0x20
    props = b""
    if session_expiry is not None:
        props += bytes([0x11]) + struct.pack("!I", session_expiry)
    vh = utf8("MQTT") + b"\x05" + bytes([flags]) + struct.pack("!H", 60) + varint(len(props)) + props
    payload = utf8(client_id)
    if will is not None:
        wprops = b""
        delay = will.get("delay", 0)
        if delay:
            wprops += bytes([0x18]) + struct.pack("!I", delay)
        payload += varint(len(wprops)) + wprops
        payload += utf8(will["topic"]) + binary(will["payload"])
    payload += utf8(user) + binary(password.encode())
    return packet(0x10, vh + payload)


def subscribe_packet(pid, topic_filter, qos=0, options_extra=0, subscription_id=None):
    props = b""
    if subscription_id is not None:
        props += b"\x0b" + varint(subscription_id)
    options = (qos & 0x03) | options_extra
    body = struct.pack("!H", pid) + varint(len(props)) + props + utf8(topic_filter) + bytes([options])
    return packet(0x82, body)


def publish_packet(topic, payload, *, qos=0, retain=False, pid=1, dup=False):
    header = 0x30 | ((qos & 0x03) << 1)
    if retain:
        header |= 0x01
    if dup:
        header |= 0x08
    body = utf8(topic)
    if qos:
        body += struct.pack("!H", pid)
    body += b"\x00" + payload
    return packet(header, body)


def ack_packet(packet_type, pid, reason=None):
    header = (packet_type << 4) | (0x02 if packet_type == 6 else 0)
    body = struct.pack("!H", pid)
    if reason is not None:
        body += bytes([reason, 0])
    return packet(header, body)


def disconnect_packet(reason=0x00):
    if reason == 0:
        return b"\xe0\x00"
    return b"\xe0\x02" + bytes([reason, 0])


class RawClient:
    def __init__(self, client_id, *, clean_start=True, session_expiry=0, will=None, timeout=3.0, user=USER, password=PASSWORD, expect_reason=0):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        self.client_id = client_id
        self.sock.sendall(connect_packet(
            client_id,
            clean_start=clean_start,
            session_expiry=session_expiry,
            will=will,
            user=user,
            password=password,
        ))
        header, body = self.read_packet()
        assert header >> 4 == 2, (header, body)
        assert len(body) >= 2 and body[1] == expect_reason, (
            f"CONNACK reason {body[1] if len(body) >= 2 else None!r}, expected {expect_reason:#x}: {body!r}"
        )
        self.session_present = bool(body[0] & 1)

    def read_exact(self, n):
        out = bytearray()
        while len(out) < n:
            part = self.sock.recv(n - len(out))
            if not part:
                raise EOFError("socket closed")
            out.extend(part)
        return bytes(out)

    def read_packet(self, timeout=None):
        if timeout is not None:
            self.sock.settimeout(timeout)
        first = self.read_exact(1)[0]
        remaining = 0
        mult = 1
        for _ in range(4):
            b = self.read_exact(1)[0]
            remaining += (b & 0x7F) * mult
            if b & 0x80 == 0:
                return first, self.read_exact(remaining)
            mult *= 128
        raise ValueError("malformed remaining length")

    def expect_type(self, packet_type, timeout=3.0):
        h, b = self.read_packet(timeout)
        assert h >> 4 == packet_type, f"expected type {packet_type}, got {h >> 4}: {h:#x} {b!r}"
        return h, b

    def subscribe(self, topic_filter, qos=0, *, options_extra=0, subscription_id=None, pid=1):
        self.sock.sendall(subscribe_packet(pid, topic_filter, qos, options_extra, subscription_id))
        _, body = self.expect_type(9)
        assert body[:2] == struct.pack("!H", pid)
        prop_len, used = read_varint_bytes(body, 2)
        reason_offset = 2 + used + prop_len
        reason = body[reason_offset]
        assert reason <= 2, f"SUBACK failure {reason:#x}"
        return reason

    def publish(self, topic, payload, *, qos=0, retain=False, pid=1):
        self.sock.sendall(publish_packet(topic, payload, qos=qos, retain=retain, pid=pid))
        if qos == 1:
            _, body = self.expect_type(4)
            assert struct.unpack("!H", body[:2])[0] == pid

    def close_abrupt(self):
        self.sock.close()

    def disconnect(self, reason=0):
        try:
            self.sock.sendall(disconnect_packet(reason))
        finally:
            self.sock.close()


def parse_publish(header, body):
    qos = (header >> 1) & 3
    retain = bool(header & 1)
    dup = bool(header & 8)
    topic_len = struct.unpack("!H", body[:2])[0]
    off = 2
    topic = body[off:off+topic_len].decode()
    off += topic_len
    pid = None
    if qos:
        pid = struct.unpack("!H", body[off:off+2])[0]
        off += 2
    prop_len, used = read_varint_bytes(body, off)
    prop_raw = body[off+used:off+used+prop_len]
    off += used + prop_len
    return {
        "qos": qos, "retain": retain, "dup": dup, "topic": topic,
        "pid": pid, "properties": prop_raw, "payload": body[off:]
    }


def no_packet(client, timeout=0.4):
    try:
        client.read_packet(timeout)
        return False
    except socket.timeout:
        return True


def test_retained():
    topic = "protocol/v2/retained"
    pub = RawClient("proto_ret_pub")
    # Make the test idempotent even though retained state survives broker restarts.
    pub.publish(topic, b"", qos=0, retain=True)
    pub.publish(topic, b"retained-value", qos=1, retain=True, pid=11)
    sub = RawClient("proto_ret_sub")
    sub.subscribe(topic, qos=1, subscription_id=77)
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["retain"] and msg["payload"] == b"retained-value" and msg["topic"] == topic
    # Subscription Identifier property 0x0B must survive retained replay.
    assert msg["properties"] and msg["properties"][0] == 0x0B, msg
    if msg["qos"] == 1:
        sub.sock.sendall(ack_packet(4, msg["pid"]))
    sub.disconnect()

    pub.publish(topic, b"", qos=0, retain=True)
    fresh = RawClient("proto_ret_empty")
    fresh.subscribe(topic, qos=0)
    assert no_packet(fresh), "deleted retained message was replayed"
    fresh.disconnect(); pub.disconnect()


def test_qos2_exactly_once():
    topic = "protocol/v2/qos2"
    sub = RawClient("proto_qos2_sub")
    sub.subscribe(topic, qos=2)
    pub = RawClient("proto_qos2_pub")
    pid = 42
    pub.sock.sendall(publish_packet(topic, b"exactly-once", qos=2, pid=pid))
    _, body = pub.expect_type(5)
    assert struct.unpack("!H", body[:2])[0] == pid
    assert no_packet(sub, 0.25), "QoS2 message routed before PUBREL"

    pub.sock.sendall(ack_packet(6, pid))
    _, body = pub.expect_type(7)
    assert struct.unpack("!H", body[:2])[0] == pid
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["qos"] == 2 and msg["payload"] == b"exactly-once"
    sub.sock.sendall(ack_packet(5, msg["pid"]))
    _, rel = sub.expect_type(6)
    assert struct.unpack("!H", rel[:2])[0] == msg["pid"]
    sub.sock.sendall(ack_packet(7, msg["pid"]))

    # Lost PUBCOMP case: duplicate PUBREL gets a completion response but no redelivery.
    pub.sock.sendall(ack_packet(6, pid))
    _, body = pub.expect_type(7)
    assert struct.unpack("!H", body[:2])[0] == pid
    assert no_packet(sub, 0.35), "duplicate QoS2 delivery after repeated PUBREL"
    pub.disconnect(); sub.disconnect()


def test_will_and_delay_cancel():
    topic = "protocol/v2/will"
    sub = RawClient("proto_will_sub")
    sub.subscribe(topic, qos=1)
    doomed = RawClient("proto_will_sender", will={"topic": topic, "payload": b"gone", "qos": 1, "delay": 0})
    doomed.close_abrupt()
    h, body = sub.expect_type(3, timeout=3)
    msg = parse_publish(h, body)
    assert msg["payload"] == b"gone" and msg["qos"] == 1
    sub.sock.sendall(ack_packet(4, msg["pid"]))

    delayed_id = "proto_will_delayed"
    delayed = RawClient(delayed_id, clean_start=True, session_expiry=10,
                        will={"topic": topic, "payload": b"should-not-fire", "qos": 0, "delay": 2})
    delayed.close_abrupt()
    time.sleep(0.35)
    resumed = RawClient(delayed_id, clean_start=False, session_expiry=10)
    assert resumed.session_present, "persistent Session was not resumed"
    assert no_packet(sub, 2.4), "delayed Will was not cancelled by Session reconnect"
    resumed.disconnect(); sub.disconnect()


def test_persistent_session():
    cid = "proto_persistent"
    topic = "protocol/v2/persistent"
    first = RawClient(cid, clean_start=True, session_expiry=20)
    first.subscribe(topic, qos=1)
    first.disconnect()
    time.sleep(0.1)
    resumed = RawClient(cid, clean_start=False, session_expiry=20)
    assert resumed.session_present, "CONNACK Session Present was 0"
    pub = RawClient("proto_persistent_pub")
    pub.publish(topic, b"still-subscribed", qos=1, pid=33)
    h, body = resumed.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["payload"] == b"still-subscribed"
    if msg["qos"] == 1:
        resumed.sock.sendall(ack_packet(4, msg["pid"]))
    pub.disconnect(); resumed.disconnect()


def test_subscription_retain_options_and_no_local():
    base = "protocol/v2/options"
    retained_topic = base + "/retained"
    pub = RawClient("proto_opts_pub")
    for topic in (retained_topic, base + "/rap", base + "/rap1"):
        pub.publish(topic, b"", qos=0, retain=True)
    pub.publish(retained_topic, b"seed", qos=1, retain=True, pid=51)

    # Retain Handling=2: never send retained messages on subscribe.
    never = RawClient("proto_opts_never")
    never.subscribe(retained_topic, qos=0, options_extra=0x20)
    assert no_packet(never), "Retain Handling=2 replayed retained state"
    never.disconnect()

    # Retain Handling=1: replay only when the subscription is new.
    only_new = RawClient("proto_opts_new")
    only_new.subscribe(retained_topic, qos=0, options_extra=0x10, pid=1)
    h, body = only_new.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["retain"] and msg["payload"] == b"seed"
    only_new.subscribe(retained_topic, qos=0, options_extra=0x10, pid=2)
    assert no_packet(only_new), "Retain Handling=1 replayed on an existing subscription"
    only_new.disconnect()

    # RAP=0 clears RETAIN on a live forwarded retained publish.
    rap0 = RawClient("proto_opts_rap0")
    rap0.subscribe(base + "/rap", qos=0)
    pub.publish(base + "/rap", b"rap0", qos=0, retain=True)
    h, body = rap0.expect_type(3)
    msg = parse_publish(h, body)
    assert not msg["retain"] and msg["payload"] == b"rap0", msg
    rap0.disconnect()

    # RAP=1 preserves RETAIN on a live forwarded retained publish.
    rap1 = RawClient("proto_opts_rap1")
    rap1.subscribe(base + "/rap1", qos=0, options_extra=0x08)
    pub.publish(base + "/rap1", b"rap1", qos=0, retain=True)
    h, body = rap1.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["retain"] and msg["payload"] == b"rap1", msg
    rap1.disconnect()

    # No Local suppresses a client's own publication but not another client's.
    local = RawClient("proto_opts_local")
    local.subscribe(base + "/nl", qos=0, options_extra=0x04)
    local.publish(base + "/nl", b"self", qos=0)
    assert no_packet(local), "No Local delivered the client's own publish"
    pub.publish(base + "/nl", b"other", qos=0)
    h, body = local.expect_type(3)
    assert parse_publish(h, body)["payload"] == b"other"
    local.disconnect(); pub.disconnect()


def test_will_normal_disconnect_and_retained():
    topic = "protocol/v2/will-retained"
    cleaner = RawClient("proto_will_rules_cleaner")
    cleaner.publish(topic, b"", qos=0, retain=True)
    cleaner.disconnect()
    sub = RawClient("proto_will_rules_sub")
    sub.subscribe(topic, qos=1)

    graceful = RawClient(
        "proto_will_graceful",
        will={"topic": topic, "payload": b"must-not-fire", "qos": 1, "delay": 0},
    )
    graceful.disconnect()
    assert no_packet(sub, 0.6), "normal DISCONNECT 0x00 published the Will"

    retained = RawClient(
        "proto_will_retained_sender",
        will={"topic": topic, "payload": b"retained-will", "qos": 1, "retain": True, "delay": 0},
    )
    retained.close_abrupt()
    h, body = sub.expect_type(3, timeout=3)
    live = parse_publish(h, body)
    # Default RAP=0 clears RETAIN on live delivery.
    assert not live["retain"] and live["payload"] == b"retained-will", live
    sub.sock.sendall(ack_packet(4, live["pid"]))

    fresh = RawClient("proto_will_retained_fresh")
    fresh.subscribe(topic, qos=1)
    h, body = fresh.expect_type(3)
    replay = parse_publish(h, body)
    assert replay["retain"] and replay["payload"] == b"retained-will", replay
    fresh.sock.sendall(ack_packet(4, replay["pid"]))
    fresh.disconnect(); sub.disconnect()


def test_outgoing_qos2_resume_from_pubrel():
    topic = "protocol/v2/qos2-outgoing-resume"
    cid = "proto_qos2_out_resume"
    sub = RawClient(cid, clean_start=True, session_expiry=30)
    sub.subscribe(topic, qos=2)
    pub = RawClient("proto_qos2_out_resume_pub")

    # Complete publisher->broker QoS2 so the broker creates an outbound QoS2 flow.
    pub.sock.sendall(publish_packet(topic, b"resume-pubrel", qos=2, pid=91))
    pub.expect_type(5)
    pub.sock.sendall(ack_packet(6, 91))
    pub.expect_type(7)

    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["qos"] == 2 and msg["payload"] == b"resume-pubrel", msg
    sub.sock.sendall(ack_packet(5, msg["pid"]))
    _, rel = sub.expect_type(6)
    assert struct.unpack("!H", rel[:2])[0] == msg["pid"]

    # Simulate losing PUBCOMP: reconnect the same persistent Session.
    sub.close_abrupt()
    time.sleep(0.15)
    resumed = RawClient(cid, clean_start=False, session_expiry=30)
    assert resumed.session_present
    _, rel2 = resumed.expect_type(6)
    assert struct.unpack("!H", rel2[:2])[0] == msg["pid"], rel2
    resumed.sock.sendall(ack_packet(7, msg["pid"]))
    assert no_packet(resumed, 0.35), "completed QoS2 state was replayed again"
    resumed.disconnect(); pub.disconnect()


def test_client_id_takeover_stress():
    cid = "proto_takeover_stress"
    current = RawClient(cid, clean_start=True, session_expiry=60)
    for i in range(20):
        replacement = RawClient(cid, clean_start=False, session_expiry=60)
        assert replacement.session_present, f"iteration {i}: Session Present=0"
        _, body = current.expect_type(14, timeout=3)
        assert body and body[0] == 0x8E, f"iteration {i}: old owner missing 0x8E"
        replacement.sock.sendall(b"\xc0\x00")
        replacement.expect_type(13)
        try:
            current.sock.close()
        except Exception:
            pass
        current = replacement
    current.disconnect()


def test_client_id_principal_binding():
    cid = "proto_principal_bound"
    owner = RawClient(cid, clean_start=True, session_expiry=60, user="admin", password="admin123")
    owner.subscribe("protocol/v2/principal/#", qos=1)

    # A different authenticated principal must not inherit or replace this Session.
    attacker = RawClient(
        cid,
        clean_start=False,
        session_expiry=60,
        user="sensor",
        password="sensor123",
        expect_reason=0x87,
    )
    assert not attacker.session_present
    attacker.sock.close()

    # Even Clean Start=1 cannot be used by another principal to erase/take over
    # an active/persistent ClientID owned under a different ACL identity.
    attacker_clean = RawClient(
        cid,
        clean_start=True,
        session_expiry=0,
        user="sensor",
        password="sensor123",
        expect_reason=0x87,
    )
    attacker_clean.sock.close()

    # Rejected attempts must not disturb the legitimate current owner.
    owner.sock.sendall(b"\xc0\x00")
    owner.expect_type(13)

    # The original principal can still perform a normal same-ClientID takeover.
    replacement = RawClient(cid, clean_start=False, session_expiry=60, user="admin", password="admin123")
    assert replacement.session_present
    _, body = owner.expect_type(14, timeout=3)
    assert body and body[0] == 0x8E
    replacement.sock.sendall(b"\xc0\x00")
    replacement.expect_type(13)
    replacement.disconnect()
    try:
        owner.sock.close()
    except Exception:
        pass


def test_client_id_takeover():
    cid = "proto_takeover"
    old = RawClient(cid, clean_start=True, session_expiry=30)
    new = RawClient(cid, clean_start=False, session_expiry=30)
    assert new.session_present, "takeover did not resume persistent Session"
    h, body = old.expect_type(14, timeout=3)
    reason = body[0] if body else 0
    assert reason == 0x8E, f"old client did not receive Session taken over: {reason:#x}"
    # New owner must remain alive after the old task cleans up.
    new.sock.sendall(b"\xc0\x00")
    new.expect_type(13)
    new.disconnect()
    try:
        old.sock.close()
    except Exception:
        pass


def run(name, fn):
    started = time.time()
    fn()
    print(f"[PASS] {name} ({time.time()-started:.3f}s)")


if __name__ == "__main__":
    tests = [
        ("retained store/replay/delete + subscription id", test_retained),
        ("QoS2 four-way exactly-once", test_qos2_exactly_once),
        ("Last Will + delayed Will cancellation", test_will_and_delay_cancel),
        ("persistent Session + Session Present", test_persistent_session),
        ("subscription Retain Handling/RAP/No Local", test_subscription_retain_options_and_no_local),
        ("Will normal suppression + retained Will", test_will_normal_disconnect_and_retained),
        ("QoS2 outbound resume from PUBREL", test_outgoing_qos2_resume_from_pubrel),
        ("ClientID principal binding / ACL isolation", test_client_id_principal_binding),
        ("ClientID takeover DISCONNECT 0x8E", test_client_id_takeover),
        ("ClientID takeover stress x20", test_client_id_takeover_stress),
    ]
    for name, fn in tests:
        run(name, fn)
    print(f"ALL {len(tests)} MQTT v5 protocol tests PASSED")

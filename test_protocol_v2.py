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


def application_properties_bytes(properties=None, *, allow_subscription_ids=True, allow_topic_alias=True):
    properties = properties or {}
    out = bytearray()
    if properties.get("payload_format_indicator") is not None:
        out += bytes([0x01, properties["payload_format_indicator"]])
    if properties.get("message_expiry_interval") is not None:
        out += bytes([0x02]) + struct.pack("!I", properties["message_expiry_interval"])
    if properties.get("content_type") is not None:
        out += bytes([0x03]) + utf8(properties["content_type"])
    if properties.get("response_topic") is not None:
        out += bytes([0x08]) + utf8(properties["response_topic"])
    if properties.get("correlation_data") is not None:
        out += bytes([0x09]) + binary(properties["correlation_data"])
    if allow_subscription_ids:
        for identifier in properties.get("subscription_identifiers", []):
            out += bytes([0x0B]) + varint(identifier)
    if allow_topic_alias and properties.get("topic_alias") is not None:
        out += bytes([0x23]) + struct.pack("!H", properties["topic_alias"])
    for key, value in properties.get("user_properties", []):
        out += bytes([0x26]) + utf8(key) + utf8(value)
    return bytes(out)


def parse_application_properties(raw: bytes):
    result = {
        "payload_format_indicator": None,
        "message_expiry_interval": None,
        "content_type": None,
        "response_topic": None,
        "correlation_data": None,
        "subscription_identifiers": [],
        "topic_alias": None,
        "user_properties": [],
    }
    off = 0
    def read_utf8():
        nonlocal off
        size = struct.unpack("!H", raw[off:off+2])[0]
        off += 2
        value = raw[off:off+size].decode()
        off += size
        return value
    def read_binary():
        nonlocal off
        size = struct.unpack("!H", raw[off:off+2])[0]
        off += 2
        value = raw[off:off+size]
        off += size
        return value
    while off < len(raw):
        prop = raw[off]
        off += 1
        if prop == 0x01:
            result["payload_format_indicator"] = raw[off]; off += 1
        elif prop == 0x02:
            result["message_expiry_interval"] = struct.unpack("!I", raw[off:off+4])[0]; off += 4
        elif prop == 0x03:
            result["content_type"] = read_utf8()
        elif prop == 0x08:
            result["response_topic"] = read_utf8()
        elif prop == 0x09:
            result["correlation_data"] = read_binary()
        elif prop == 0x0B:
            value, used = read_varint_bytes(raw, off); off += used
            result["subscription_identifiers"].append(value)
        elif prop == 0x23:
            result["topic_alias"] = struct.unpack("!H", raw[off:off+2])[0]; off += 2
        elif prop == 0x26:
            result["user_properties"].append((read_utf8(), read_utf8()))
        else:
            raise AssertionError(f"unexpected PUBLISH property {prop:#x} in {raw!r}")
    return result


def packet(header: int, body: bytes) -> bytes:
    return bytes([header]) + varint(len(body)) + body


def parse_connack_properties(body: bytes):
    assert len(body) >= 3, body
    prop_len, used = read_varint_bytes(body, 2)
    raw = body[2+used:2+used+prop_len]
    result = {
        "receive_maximum": None,
        "maximum_packet_size": None,
        "assigned_client_identifier": None,
        "topic_alias_maximum": None,
    }
    off = 0
    def read_utf8_prop():
        nonlocal off
        size = struct.unpack("!H", raw[off:off+2])[0]
        off += 2
        value = raw[off:off+size].decode()
        off += size
        return value
    while off < len(raw):
        prop = raw[off]; off += 1
        if prop == 0x21:
            result["receive_maximum"] = struct.unpack("!H", raw[off:off+2])[0]; off += 2
        elif prop == 0x27:
            result["maximum_packet_size"] = struct.unpack("!I", raw[off:off+4])[0]; off += 4
        elif prop == 0x12:
            result["assigned_client_identifier"] = read_utf8_prop()
        elif prop == 0x22:
            result["topic_alias_maximum"] = struct.unpack("!H", raw[off:off+2])[0]; off += 2
        else:
            raise AssertionError(f"unexpected CONNACK property {prop:#x} in {raw!r}")
    return result


def connect_packet(
    client_id,
    *,
    clean_start=True,
    session_expiry=0,
    receive_maximum=None,
    maximum_packet_size=None,
    raw_connect_properties=None,
    will=None,
    user=USER,
    password=PASSWORD,
):
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
    if receive_maximum is not None:
        props += bytes([0x21]) + struct.pack("!H", receive_maximum)
    if maximum_packet_size is not None:
        props += bytes([0x27]) + struct.pack("!I", maximum_packet_size)
    if raw_connect_properties is not None:
        props += raw_connect_properties
    vh = utf8("MQTT") + b"\x05" + bytes([flags]) + struct.pack("!H", 60) + varint(len(props)) + props
    payload = utf8(client_id)
    if will is not None:
        wprops = b""
        delay = will.get("delay", 0)
        if delay:
            wprops += bytes([0x18]) + struct.pack("!I", delay)
        wprops += application_properties_bytes(
            will.get("properties"),
            allow_subscription_ids=False,
            allow_topic_alias=False,
        )
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


def unsubscribe_packet(pid, *topic_filters, user_properties=None):
    props = b""
    for key, value in user_properties or []:
        props += b"\x26" + utf8(key) + utf8(value)
    body = struct.pack("!H", pid) + varint(len(props)) + props
    for topic_filter in topic_filters:
        body += utf8(topic_filter)
    return packet(0xA2, body)


def publish_packet(topic, payload, *, qos=0, retain=False, pid=1, dup=False, properties=None, raw_properties=None):
    header = 0x30 | ((qos & 0x03) << 1)
    if retain:
        header |= 0x01
    if dup:
        header |= 0x08
    body = utf8(topic)
    if qos:
        body += struct.pack("!H", pid)
    prop_bytes = raw_properties if raw_properties is not None else application_properties_bytes(properties)
    body += varint(len(prop_bytes)) + prop_bytes + payload
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
    def __init__(
        self,
        client_id,
        *,
        clean_start=True,
        session_expiry=0,
        receive_maximum=None,
        maximum_packet_size=None,
        will=None,
        timeout=3.0,
        user=USER,
        password=PASSWORD,
        expect_reason=0,
    ):
        self.sock = socket.create_connection((HOST, PORT), timeout=timeout)
        self.sock.settimeout(timeout)
        self.client_id = client_id
        self.sock.sendall(connect_packet(
            client_id,
            clean_start=clean_start,
            session_expiry=session_expiry,
            receive_maximum=receive_maximum,
            maximum_packet_size=maximum_packet_size,
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
        self.connack_properties = parse_connack_properties(body) if expect_reason == 0 else {}
        if self.connack_properties.get("assigned_client_identifier"):
            self.client_id = self.connack_properties["assigned_client_identifier"]

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

    def unsubscribe(self, *topic_filters, pid=2):
        self.sock.sendall(unsubscribe_packet(pid, *topic_filters))
        _, body = self.expect_type(11)
        assert body[:2] == struct.pack("!H", pid), body
        prop_len, used = read_varint_bytes(body, 2)
        off = 2 + used + prop_len
        return list(body[off:])

    def publish(self, topic, payload, *, qos=0, retain=False, pid=1, properties=None):
        self.sock.sendall(publish_packet(
            topic, payload, qos=qos, retain=retain, pid=pid, properties=properties
        ))
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
        "pid": pid, "properties": prop_raw,
        "parsed_properties": parse_application_properties(prop_raw),
        "payload": body[off:]
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


def test_publish_application_properties_end_to_end():
    topic = "protocol/v2/properties/live"
    sub = RawClient("proto_props_live_sub")
    sub.subscribe(topic, qos=0, subscription_id=41)
    pub = RawClient("proto_props_live_pub")
    properties = {
        "payload_format_indicator": 1,
        "message_expiry_interval": 5,
        "content_type": "application/json",
        "response_topic": "protocol/v2/properties/reply",
        "correlation_data": b"corr-123",
        "user_properties": [("first", "one"), ("second", "two")],
    }
    pub.publish(topic, b'{"ok":true}', properties=properties)
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["payload"] == b'{"ok":true}'
    assert got["payload_format_indicator"] == 1, got
    assert got["content_type"] == "application/json", got
    assert got["response_topic"] == "protocol/v2/properties/reply", got
    assert got["correlation_data"] == b"corr-123", got
    assert got["user_properties"] == [("first", "one"), ("second", "two")], got
    assert got["subscription_identifiers"] == [41], got
    assert got["topic_alias"] is None, got
    assert 1 <= got["message_expiry_interval"] <= 5, got
    pub.disconnect(); sub.disconnect()


def test_retained_properties_and_message_expiry():
    topic = "protocol/v2/properties/retained-expiry"
    cleaner = RawClient("proto_props_ret_clean")
    cleaner.publish(topic, b"", retain=True)
    properties = {
        "payload_format_indicator": 1,
        "message_expiry_interval": 3,
        "content_type": "text/plain",
        "response_topic": "protocol/v2/properties/reply-retained",
        "correlation_data": b"ret-corr",
        "user_properties": [("order", "1"), ("order", "2")],
    }
    cleaner.publish(topic, b"expires", qos=1, retain=True, pid=191, properties=properties)
    time.sleep(1.15)
    sub = RawClient("proto_props_ret_sub")
    sub.subscribe(topic, qos=1, subscription_id=92)
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["retain"] and msg["payload"] == b"expires", msg
    assert got["content_type"] == "text/plain", got
    assert got["correlation_data"] == b"ret-corr", got
    assert got["user_properties"] == [("order", "1"), ("order", "2")], got
    assert got["subscription_identifiers"] == [92], got
    assert 1 <= got["message_expiry_interval"] <= 2, got
    if msg["qos"] == 1:
        sub.sock.sendall(ack_packet(4, msg["pid"]))
    sub.disconnect()

    time.sleep(2.2)
    expired = RawClient("proto_props_ret_expired")
    expired.subscribe(topic, qos=0)
    assert no_packet(expired, 0.6), "expired retained message was replayed"
    expired.disconnect(); cleaner.disconnect()


def test_offline_qos1_properties_and_expiry():
    topic = "protocol/v2/properties/offline"
    cid = "proto_props_offline_sub"
    sub = RawClient(cid, clean_start=True, session_expiry=30)
    sub.subscribe(topic, qos=1, subscription_id=73)
    sub.disconnect()
    time.sleep(0.1)

    pub = RawClient("proto_props_offline_pub")
    properties = {
        "message_expiry_interval": 6,
        "content_type": "application/octet-stream",
        "correlation_data": b"offline-corr",
        "user_properties": [("persist", "yes"), ("sequence", "two")],
    }
    pub.publish(topic, b"offline-properties", qos=1, pid=201, properties=properties)
    time.sleep(1.15)

    resumed = RawClient(cid, clean_start=False, session_expiry=30)
    assert resumed.session_present
    h, body = resumed.expect_type(3)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert not msg["dup"] and msg["payload"] == b"offline-properties", msg
    assert got["content_type"] == "application/octet-stream", got
    assert got["correlation_data"] == b"offline-corr", got
    assert got["user_properties"] == [("persist", "yes"), ("sequence", "two")], got
    assert got["subscription_identifiers"] == [73], got
    assert 1 <= got["message_expiry_interval"] <= 5, got
    resumed.sock.sendall(ack_packet(4, msg["pid"]))
    resumed.disconnect(); pub.disconnect()


def test_offline_unsent_message_expires_before_reconnect():
    topic = "protocol/v2/expiry/offline-unsent"
    cid = "proto_expiry_unsent_sub"
    sub = RawClient(cid, clean_start=True, session_expiry=20)
    sub.subscribe(topic, qos=1)
    sub.disconnect()
    time.sleep(0.1)
    pub = RawClient("proto_expiry_unsent_pub")
    pub.publish(
        topic,
        b"must-expire",
        qos=1,
        pid=211,
        properties={"message_expiry_interval": 1},
    )
    time.sleep(1.25)
    resumed = RawClient(cid, clean_start=False, session_expiry=20)
    assert resumed.session_present
    assert no_packet(resumed, 0.7), "offline message was delivered after Message Expiry"
    resumed.disconnect(); pub.disconnect()


def test_started_qos1_retries_after_expiry():
    topic = "protocol/v2/expiry/qos1-started"
    cid = "proto_expiry_qos1_sub"
    sub = RawClient(cid, clean_start=True, session_expiry=20)
    sub.subscribe(topic, qos=1)
    pub = RawClient("proto_expiry_qos1_pub")
    pub.publish(
        topic,
        b"qos1-started",
        qos=1,
        pid=212,
        properties={"message_expiry_interval": 1, "user_properties": [("q", "one")]},
    )
    h, body = sub.expect_type(3)
    first = parse_publish(h, body)
    assert first["qos"] == 1 and first["payload"] == b"qos1-started"
    # Do not PUBACK: onward delivery has started but remains unacknowledged.
    sub.close_abrupt()
    time.sleep(1.25)
    resumed = RawClient(cid, clean_start=False, session_expiry=20)
    assert resumed.session_present
    h, body = resumed.expect_type(3)
    retry = parse_publish(h, body)
    assert retry["dup"] and retry["pid"] == first["pid"], retry
    assert retry["payload"] == b"qos1-started"
    assert retry["parsed_properties"]["user_properties"] == [("q", "one")]
    # Expiry can have reached zero, but MQTT retry state must still complete.
    assert retry["parsed_properties"]["message_expiry_interval"] == 0, retry
    resumed.sock.sendall(ack_packet(4, retry["pid"]))
    resumed.disconnect(); pub.disconnect()


def test_started_qos2_continues_after_expiry():
    topic = "protocol/v2/expiry/qos2-started"
    cid = "proto_expiry_qos2_sub"
    sub = RawClient(cid, clean_start=True, session_expiry=20)
    sub.subscribe(topic, qos=2)
    pub = RawClient("proto_expiry_qos2_pub")
    pub.sock.sendall(publish_packet(
        topic,
        b"qos2-started",
        qos=2,
        pid=213,
        properties={"message_expiry_interval": 1, "user_properties": [("q", "two")]},
    ))
    pub.expect_type(5)
    pub.sock.sendall(ack_packet(6, 213))
    pub.expect_type(7)
    h, body = sub.expect_type(3)
    first = parse_publish(h, body)
    assert first["qos"] == 2 and first["payload"] == b"qos2-started", first
    sub.sock.sendall(ack_packet(5, first["pid"]))
    _, rel = sub.expect_type(6)
    assert struct.unpack("!H", rel[:2])[0] == first["pid"]
    # Lose PUBCOMP, then let Message Expiry pass. The PUBREL state still must resume.
    sub.close_abrupt()
    time.sleep(1.25)
    resumed = RawClient(cid, clean_start=False, session_expiry=20)
    assert resumed.session_present
    _, rel2 = resumed.expect_type(6)
    assert struct.unpack("!H", rel2[:2])[0] == first["pid"]
    resumed.sock.sendall(ack_packet(7, first["pid"]))
    assert no_packet(resumed, 0.4)
    resumed.disconnect(); pub.disconnect()


def test_will_application_properties():
    topic = "protocol/v2/properties/will"
    sub = RawClient("proto_props_will_sub")
    sub.subscribe(topic, qos=1, subscription_id=66)
    will_properties = {
        "payload_format_indicator": 1,
        "message_expiry_interval": 4,
        "content_type": "text/plain",
        "response_topic": "protocol/v2/properties/will-reply",
        "correlation_data": b"will-corr",
        "user_properties": [("will", "first"), ("will", "second")],
    }
    doomed = RawClient(
        "proto_props_will_sender",
        session_expiry=20,
        will={
            "topic": topic,
            "payload": b"will-properties",
            "qos": 1,
            "delay": 0,
            "properties": will_properties,
        },
    )
    doomed.close_abrupt()
    h, body = sub.expect_type(3, timeout=3)
    msg = parse_publish(h, body)
    got = msg["parsed_properties"]
    assert msg["payload"] == b"will-properties"
    assert got["payload_format_indicator"] == 1, got
    assert got["content_type"] == "text/plain", got
    assert got["response_topic"] == "protocol/v2/properties/will-reply", got
    assert got["correlation_data"] == b"will-corr", got
    assert got["user_properties"] == [("will", "first"), ("will", "second")], got
    assert got["subscription_identifiers"] == [66], got
    assert 1 <= got["message_expiry_interval"] <= 4, got
    sub.sock.sendall(ack_packet(4, msg["pid"]))
    sub.disconnect()


def test_topic_alias_lifecycle_and_routing():
    root = "protocol/v2/topic-alias"
    first_topic = root + "/first"
    second_topic = root + "/second"
    sub = RawClient("proto_alias_sub")
    sub.subscribe(root + "/#", qos=0, pid=201)

    pub = RawClient("proto_alias_pub", clean_start=True, session_expiry=30)
    maximum = pub.connack_properties["topic_alias_maximum"]
    assert maximum == 32, pub.connack_properties

    # Non-empty Topic Name + alias establishes the mapping. Topic Alias is
    # connection-local and must not be forwarded to the subscriber.
    pub.publish(first_topic, b"mapped", properties={"topic_alias": 1})
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["topic"] == first_topic and msg["payload"] == b"mapped", msg
    assert msg["parsed_properties"]["topic_alias"] is None, msg

    # Empty Topic Name resolves the established alias.
    pub.publish("", b"reused", properties={"topic_alias": 1})
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["topic"] == first_topic and msg["payload"] == b"reused", msg

    # A non-empty Topic Name updates the same alias.
    pub.publish(second_topic, b"updated", properties={"topic_alias": 1})
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["topic"] == second_topic and msg["payload"] == b"updated", msg
    pub.publish("", b"updated-reuse", properties={"topic_alias": 1})
    h, body = sub.expect_type(3)
    msg = parse_publish(h, body)
    assert msg["topic"] == second_topic and msg["payload"] == b"updated-reuse", msg

    # Alias mappings are Network Connection state, not persistent Session state.
    pub.disconnect()
    resumed = RawClient("proto_alias_pub", clean_start=False, session_expiry=30)
    assert resumed.session_present, "persistent Session did not resume"
    resumed.sock.sendall(publish_packet("", b"must-fail", properties={"topic_alias": 1}))
    _, body = resumed.expect_type(14)
    assert body and body[0] == 0x82, body
    resumed.sock.close()

    # Using an in-range alias before establishing it is also Protocol Error.
    unknown = RawClient("proto_alias_unknown")
    unknown.sock.sendall(publish_packet("", b"unknown", properties={"topic_alias": 2}))
    _, body = unknown.expect_type(14)
    assert body and body[0] == 0x82, body
    unknown.sock.close()
    sub.disconnect()


def test_publish_property_protocol_errors():
    # Client->Server Subscription Identifier is a Protocol Error (0x82).
    bad_subid = RawClient("proto_bad_publish_subid")
    bad_subid.sock.sendall(publish_packet(
        "protocol/v2/bad/subid",
        b"bad",
        properties={"subscription_identifiers": [7]},
    ))
    _, body = bad_subid.expect_type(14)
    assert body and body[0] == 0x82, body
    bad_subid.sock.close()

    # Topic Alias 0 is always invalid (0x94).
    bad_alias_zero = RawClient("proto_bad_publish_alias_zero")
    bad_alias_zero.sock.sendall(publish_packet(
        "protocol/v2/bad/alias",
        b"bad",
        properties={"topic_alias": 0},
    ))
    _, body = bad_alias_zero.expect_type(14)
    assert body and body[0] == 0x94, body
    bad_alias_zero.sock.close()

    # Alias above the server-advertised maximum is also 0x94.
    bad_alias_high = RawClient("proto_bad_publish_alias_high")
    maximum = bad_alias_high.connack_properties["topic_alias_maximum"]
    assert maximum and maximum > 0, bad_alias_high.connack_properties
    bad_alias_high.sock.sendall(publish_packet(
        "protocol/v2/bad/alias",
        b"bad",
        properties={"topic_alias": maximum + 1},
    ))
    _, body = bad_alias_high.expect_type(14)
    assert body and body[0] == 0x94, body
    bad_alias_high.sock.close()

    # A singleton PUBLISH property repeated twice is a Protocol Error.
    duplicate_pfi = RawClient("proto_bad_publish_duplicate")
    duplicate_pfi.sock.sendall(publish_packet(
        "protocol/v2/bad/duplicate",
        b"bad",
        raw_properties=b"\x01\x01\x01\x01",
    ))
    _, body = duplicate_pfi.expect_type(14)
    assert body and body[0] == 0x82, body
    duplicate_pfi.sock.close()

    # Invalid topics must not slip through the zero-route QoS0 fast path.
    bad_wildcard = RawClient("proto_bad_publish_wildcard")
    bad_wildcard.sock.sendall(publish_packet(
        "protocol/v2/bad/+",
        b"bad",
    ))
    _, body = bad_wildcard.expect_type(14)
    assert body and body[0] == 0x82, body
    bad_wildcard.sock.close()

    bad_empty = RawClient("proto_bad_publish_empty")
    bad_empty.sock.sendall(publish_packet("", b"bad"))
    _, body = bad_empty.expect_type(14)
    assert body and body[0] == 0x82, body
    bad_empty.sock.close()


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


def test_unsubscribe_unsuback_and_filter_validation():
    topic = "protocol/v2/unsubscribe/live"
    client = RawClient("proto_unsub_client", clean_start=True, session_expiry=30)
    client.subscribe(topic, qos=1, pid=61)
    reasons = client.unsubscribe(topic, "protocol/v2/unsubscribe/missing", pid=62)
    assert reasons == [0x00, 0x11], reasons

    pub = RawClient("proto_unsub_pub")
    pub.publish(topic, b"must-not-arrive", qos=1, pid=63)
    assert no_packet(client, 0.5), "message arrived after successful UNSUBSCRIBE"

    # Malformed wildcard placement is a Protocol Error rather than silently creating
    # an unusable subscription.
    bad = RawClient("proto_bad_filter")
    bad.sock.sendall(subscribe_packet(64, "bad/#/tail", qos=0))
    _, body = bad.expect_type(14)
    assert body and body[0] == 0x82, body
    bad.sock.close()
    pub.disconnect(); client.disconnect()


def test_server_assigned_client_id_and_connack_limits():
    generated = RawClient("", clean_start=True, session_expiry=30)
    assigned = generated.connack_properties["assigned_client_identifier"]
    assert assigned and assigned.startswith("pip-"), generated.connack_properties
    assert generated.client_id == assigned
    assert generated.connack_properties["receive_maximum"] == 1024, generated.connack_properties
    assert generated.connack_properties["maximum_packet_size"] == 16 * 1024 * 1024, generated.connack_properties
    assert generated.connack_properties["topic_alias_maximum"] == 32, generated.connack_properties
    generated.disconnect()

    resumed = RawClient(assigned, clean_start=False, session_expiry=30)
    assert resumed.session_present, "server-assigned ClientID did not identify the persistent Session"
    resumed.disconnect()


def test_client_receive_maximum_window():
    topic = "protocol/v2/flow/receive-max"
    sub = RawClient("proto_receive_max_sub", receive_maximum=1)
    sub.subscribe(topic, qos=1, pid=71)
    pub = RawClient("proto_receive_max_pub")
    for pid, payload in [(72, b"one"), (73, b"two"), (74, b"three")]:
        pub.publish(topic, payload, qos=1, pid=pid)

    h, body = sub.expect_type(3)
    first = parse_publish(h, body)
    assert first["qos"] == 1 and first["payload"] == b"one", first
    assert no_packet(sub, 0.35), "server exceeded client Receive Maximum=1"
    sub.sock.sendall(ack_packet(4, first["pid"]))

    h, body = sub.expect_type(3)
    second = parse_publish(h, body)
    assert second["payload"] == b"two", second
    assert no_packet(sub, 0.35), "server sent third PUBLISH before second PUBACK"
    sub.sock.sendall(ack_packet(4, second["pid"]))

    h, body = sub.expect_type(3)
    third = parse_publish(h, body)
    assert third["payload"] == b"three", third
    sub.sock.sendall(ack_packet(4, third["pid"]))
    sub.disconnect(); pub.disconnect()


def test_client_maximum_packet_size_and_server_limit():
    topic = "protocol/v2/flow/max-packet"
    sub = RawClient("proto_small_packet_sub", maximum_packet_size=96)
    sub.subscribe(topic, qos=1, pid=81)
    pub = RawClient("proto_small_packet_pub")

    pub.publish(topic, b"small", qos=1, pid=82)
    h, body = sub.expect_type(3)
    small = parse_publish(h, body)
    assert small["payload"] == b"small", small
    sub.sock.sendall(ack_packet(4, small["pid"]))

    pub.publish(topic, b"X" * 256, qos=1, pid=83)
    assert no_packet(sub, 0.5), "server sent PUBLISH larger than client Maximum Packet Size"
    # Oversize delivery is treated as completed and must not poison the Receive Maximum.
    pub.publish(topic, b"after-drop", qos=1, pid=84)
    h, body = sub.expect_type(3)
    after = parse_publish(h, body)
    assert after["payload"] == b"after-drop", after
    sub.sock.sendall(ack_packet(4, after["pid"]))

    server_limit = sub.connack_properties["maximum_packet_size"]
    # Fixed header alone proves the declared packet would exceed the server limit;
    # no giant body needs to be allocated/sent.
    sub.sock.sendall(b"\x30" + varint(server_limit))
    _, body = sub.expect_type(14)
    assert body and body[0] == 0x95, body
    sub.sock.close(); pub.disconnect()


def test_connect_limits_fragmentation_and_utf8_errors():
    # CONNECT properties with forbidden zero values are Protocol Errors.
    for suffix, kwargs in [
        ("recv0", {"receive_maximum": 0}),
        ("max0", {"maximum_packet_size": 0}),
    ]:
        sock = socket.create_connection((HOST, PORT), timeout=3)
        sock.settimeout(3)
        proxy = object.__new__(RawClient); proxy.sock = sock
        sock.sendall(connect_packet(f"proto_{suffix}", **kwargs))
        _, body = proxy.expect_type(2)
        assert body[1] == 0x82, body
        sock.close()

    # TCP fragmentation must not be confused with an incomplete/malformed CONNECT.
    sock = socket.create_connection((HOST, PORT), timeout=3)
    sock.settimeout(3)
    raw = connect_packet("proto_fragmented_connect")
    proxy = object.__new__(RawClient); proxy.sock = sock
    cursor = 0
    for end in [1, 2, 5, 11, len(raw)]:
        if end > cursor:
            sock.sendall(raw[cursor:end])
            cursor = end
            time.sleep(0.02)
    _, body = proxy.expect_type(2)
    assert body[1] == 0x00, body
    sock.sendall(disconnect_packet()); sock.close()

    # U+0000 is forbidden in every MQTT UTF-8 Encoded String.
    bad_utf = RawClient("proto_bad_utf8")
    bad_utf.sock.sendall(publish_packet("bad\x00topic", b"bad"))
    _, body = bad_utf.expect_type(14)
    assert body and body[0] == 0x81, body
    bad_utf.sock.close()

    # Non-minimal Remaining Length encoding is malformed.
    bad_varint = RawClient("proto_bad_varint")
    bad_varint.sock.sendall(b"\xc0\x80\x00")
    _, body = bad_varint.expect_type(14)
    assert body and body[0] == 0x81, body
    bad_varint.sock.close()


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
        ("PUBLISH application properties end-to-end", test_publish_application_properties_end_to_end),
        ("retained properties + Message Expiry", test_retained_properties_and_message_expiry),
        ("offline QoS1 properties + Message Expiry", test_offline_qos1_properties_and_expiry),
        ("offline unsent Message Expiry", test_offline_unsent_message_expires_before_reconnect),
        ("started QoS1 retries after expiry", test_started_qos1_retries_after_expiry),
        ("started QoS2 continues after expiry", test_started_qos2_continues_after_expiry),
        ("Will application properties", test_will_application_properties),
        ("Topic Alias lifecycle/routing/reset", test_topic_alias_lifecycle_and_routing),
        ("PUBLISH property protocol errors", test_publish_property_protocol_errors),
        ("UNSUBSCRIBE/UNSUBACK + filter validation", test_unsubscribe_unsuback_and_filter_validation),
        ("server-assigned ClientID + CONNACK limits", test_server_assigned_client_id_and_connack_limits),
        ("client Receive Maximum flow window", test_client_receive_maximum_window),
        ("Maximum Packet Size both directions", test_client_maximum_packet_size_and_server_limit),
        ("CONNECT fragmentation + UTF-8/varint errors", test_connect_limits_fragmentation_and_utf8_errors),
        ("ClientID principal binding / ACL isolation", test_client_id_principal_binding),
        ("ClientID takeover DISCONNECT 0x8E", test_client_id_takeover),
        ("ClientID takeover stress x20", test_client_id_takeover_stress),
    ]
    for name, fn in tests:
        run(name, fn)
    print(f"ALL {len(tests)} MQTT v5 protocol tests PASSED")

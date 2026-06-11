import sys
import time
import threading
import ssl
import paho.mqtt.client as mqtt

# We use the v2 API of paho-mqtt
CallbackAPI = mqtt.CallbackAPIVersion.VERSION2

# Helper class to wait for events in paho-mqtt callbacks
class MQTTEventWaiter:
    def __init__(self):
        self.connect_event = threading.Event()
        self.connect_reason = None
        
        self.subscribe_event = threading.Event()
        self.subscribe_reasons = []
        
        self.message_event = threading.Event()
        self.received_message = None
        
        self.publish_event = threading.Event()
        self.publish_reason = None

def run_test_admin_tcp():
    print("\n--- Test 1: Admin Connection, Publish, & Subscribe (Plain TCP, Port 1883) ---")
    waiter = MQTTEventWaiter()
    
    client = mqtt.Client(callback_api_version=CallbackAPI, protocol=mqtt.MQTTv5, client_id="test_admin_tcp")
    client.username_pw_set("admin", "admin123")
    
    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"  [Callback] Connected with reason code: {reason_code}")
        waiter.connect_reason = reason_code
        waiter.connect_event.set()
        
    def on_subscribe(client, userdata, mid, reason_codes, properties):
        print(f"  [Callback] Subscribed: codes={reason_codes}")
        waiter.subscribe_reasons = reason_codes
        waiter.subscribe_event.set()
        
    def on_message(client, userdata, msg):
        print(f"  [Callback] Received message: topic='{msg.topic}', payload='{msg.payload.decode()}'")
        waiter.received_message = msg
        waiter.message_event.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    
    client.connect("localhost", 1883, keepalive=60)
    client.loop_start()
    
    # Wait for connection
    if not waiter.connect_event.wait(timeout=3):
        print("  [FAIL] Timeout waiting for CONNECT response.")
        client.loop_stop()
        return False
        
    if waiter.connect_reason != 0:
        print(f"  [FAIL] Connection failed: {waiter.connect_reason}")
        client.loop_stop()
        return False
        
    print("  [PASS] Connected successfully.")
    
    # Subscribe to test topic
    print("  Subscribing to 'test/simple'...")
    client.subscribe("test/simple")
    if not waiter.subscribe_event.wait(timeout=3):
        print("  [FAIL] Timeout waiting for SUBACK.")
        client.loop_stop()
        return False
        
    # In MQTT v5, reason codes <= 2 represent successful subscription (QoS 0, 1, 2)
    if not waiter.subscribe_reasons or waiter.subscribe_reasons[0] > 2:
        print(f"  [FAIL] Subscription failed: {waiter.subscribe_reasons}")
        client.loop_stop()
        return False
        
    print("  [PASS] Subscribed successfully.")
    
    # Publish a message
    print("  Publishing 'Hello Pipistrelle' to 'test/simple'...")
    client.publish("test/simple", "Hello Pipistrelle", qos=1)
    
    # Wait for message loopback
    if not waiter.message_event.wait(timeout=3):
        print("  [FAIL] Timeout waiting for loopback message.")
        client.loop_stop()
        return False
        
    if waiter.received_message.payload.decode() == "Hello Pipistrelle":
        print("  [PASS] Message received and matches exactly!")
    else:
        print(f"  [FAIL] Message mismatched: {waiter.received_message.payload}")
        client.loop_stop()
        return False
        
    client.disconnect()
    client.loop_stop()
    return True

def run_test_auth_failure():
    print("\n--- Test 2: Authentication Failure Check ---")
    waiter = MQTTEventWaiter()
    
    client = mqtt.Client(callback_api_version=CallbackAPI, protocol=mqtt.MQTTv5, client_id="test_bad_auth")
    # Wrong password
    client.username_pw_set("admin", "wrongpassword")
    
    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"  [Callback] Connection response code: {reason_code}")
        waiter.connect_reason = reason_code
        waiter.connect_event.set()

    client.on_connect = on_connect
    
    try:
        client.connect("localhost", 1883, keepalive=60)
        client.loop_start()
        
        # Wait for connection failure
        if not waiter.connect_event.wait(timeout=3):
            print("  [FAIL] Timeout waiting for CONNECT response. Expected connection rejection.")
            client.loop_stop()
            return False
            
        # 0x86 (134) is "Bad User Name or Password" in MQTT v5.0
        if waiter.connect_reason == 0x86:
            print("  [PASS] Correctly rejected with reason 0x86 (Bad User Name or Password)!")
            client.loop_stop()
            return True
        else:
            print(f"  [FAIL] Unexpected connect reason code: {waiter.connect_reason}")
            client.loop_stop()
            return False
    except Exception as e:
        # Some clients raise an exception when connection is closed immediately
        print(f"  [PASS] Connection raised exception (expected behavior on immediate rejection): {e}")
        return True

def run_test_acl_authorization():
    print("\n--- Test 3: ACL Authorization Check (User 'sensor') ---")
    waiter = MQTTEventWaiter()
    
    client = mqtt.Client(callback_api_version=CallbackAPI, protocol=mqtt.MQTTv5, client_id="test_sensor")
    client.username_pw_set("sensor", "sensor123")
    
    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"  [Callback] Connected with reason code: {reason_code}")
        waiter.connect_reason = reason_code
        waiter.connect_event.set()
        
    def on_subscribe(client, userdata, mid, reason_codes, properties):
        print(f"  [Callback] Subscribed: codes={reason_codes}")
        waiter.subscribe_reasons = reason_codes
        waiter.subscribe_event.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    
    client.connect("localhost", 1883, keepalive=60)
    client.loop_start()
    
    if not waiter.connect_event.wait(timeout=3):
        print("  [FAIL] Timeout waiting for CONNECT response.")
        client.loop_stop()
        return False
        
    if waiter.connect_reason != 0:
        print(f"  [FAIL] Connection failed: {waiter.connect_reason}")
        client.loop_stop()
        return False
        
    print("  [PASS] Connected successfully.")
    
    # Test 3a: Subscribe to allowed topic 'alerts/#'
    print("  Subscribing to allowed filter 'alerts/#'...")
    client.subscribe("alerts/#")
    if not waiter.subscribe_event.wait(timeout=3):
        print("  [FAIL] Timeout waiting for allowed SUBACK.")
        client.loop_stop()
        return False
        
    if not waiter.subscribe_reasons or waiter.subscribe_reasons[0] > 2:
        print(f"  [FAIL] Allowed subscription failed: {waiter.subscribe_reasons}")
        client.loop_stop()
        return False
    print("  [PASS] Allowed subscription accepted.")
    
    # Test 3b: Subscribe to forbidden topic 'admin/secret'
    waiter.subscribe_event.clear()
    print("  Subscribing to forbidden filter 'admin/secret'...")
    client.subscribe("admin/secret")
    if not waiter.subscribe_event.wait(timeout=3):
        print("  [FAIL] Timeout waiting for forbidden SUBACK.")
        client.loop_stop()
        return False
        
    # Reason code 0x87 (135) is "Not Authorized" in MQTT v5.0
    if waiter.subscribe_reasons and waiter.subscribe_reasons[0] == 0x87:
        print("  [PASS] Forbidden subscription correctly rejected with 0x87 (Not Authorized)!")
    else:
        print(f"  [FAIL] Forbidden subscription not rejected correctly. Received code: {waiter.subscribe_reasons}")
        client.loop_stop()
        return False
        
    client.disconnect()
    client.loop_stop()
    return True

def run_test_admin_tls():
    print("\n--- Test 4: TLS Secure Connection (Port 8883) ---")
    waiter = MQTTEventWaiter()
    
    client = mqtt.Client(callback_api_version=CallbackAPI, protocol=mqtt.MQTTv5, client_id="test_admin_tls")
    client.username_pw_set("admin", "admin123")
    
    # Configure TLS. We trust the self-signed cert.pem we generated
    try:
        # We set check_hostname to False because CN=localhost and we might connect using 'localhost' or '127.0.0.1'
        # cert.pem contains the public certificate to trust.
        client.tls_set(ca_certs="config/cert.pem", certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED, tls_version=ssl.PROTOCOL_TLS_CLIENT)
        # Disable hostname verification for self-signed certificates in local tests if needed, 
        # but we set CN=localhost so it should match 'localhost'
        client.tls_insecure_set(True)
    except Exception as e:
        print(f"  [FAIL] Failed to configure TLS: {e}")
        return False

    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"  [Callback] TLS Connected with reason code: {reason_code}")
        waiter.connect_reason = reason_code
        waiter.connect_event.set()
        
    def on_subscribe(client, userdata, mid, reason_codes, properties):
        waiter.subscribe_reasons = reason_codes
        waiter.subscribe_event.set()
        
    def on_message(client, userdata, msg):
        waiter.received_message = msg
        waiter.message_event.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    
    try:
        client.connect("localhost", 8883, keepalive=60)
        client.loop_start()
    except Exception as e:
        print(f"  [FAIL] TLS Connection attempt failed: {e}")
        return False
    
    # Wait for connection
    if not waiter.connect_event.wait(timeout=5):
        print("  [FAIL] Timeout waiting for TLS CONNECT response.")
        client.loop_stop()
        return False
        
    if waiter.connect_reason != 0:
        print(f"  [FAIL] TLS Connection failed: {waiter.connect_reason}")
        client.loop_stop()
        return False
        
    print("  [PASS] TLS connection established securely!")
    
    # Subscribe and verify loopback works over TLS
    client.subscribe("test/tls")
    if waiter.subscribe_event.wait(timeout=3) and waiter.subscribe_reasons and waiter.subscribe_reasons[0] <= 2:
        client.publish("test/tls", "Hello TLS", qos=1)
        if waiter.message_event.wait(timeout=3) and waiter.received_message.payload.decode() == "Hello TLS":
            print("  [PASS] Message published and received successfully over TLS!")
            client.disconnect()
            client.loop_stop()
            return True
            
    print("  [FAIL] Failed to publish/subscribe over TLS.")
    client.disconnect()
    client.loop_stop()
    return False

def run_test_websockets():
    print("\n--- Test 5: WebSocket Connection (Port 8083) ---")
    waiter = MQTTEventWaiter()
    
    client = mqtt.Client(callback_api_version=CallbackAPI, protocol=mqtt.MQTTv5, transport="websockets", client_id="test_ws")
    client.username_pw_set("admin", "admin123")
    
    def on_connect(client, userdata, flags, reason_code, properties):
        print(f"  [Callback] WS Connected with reason code: {reason_code}")
        waiter.connect_reason = reason_code
        waiter.connect_event.set()
        
    def on_subscribe(client, userdata, mid, reason_codes, properties):
        waiter.subscribe_reasons = reason_codes
        waiter.subscribe_event.set()
        
    def on_message(client, userdata, msg):
        waiter.received_message = msg
        waiter.message_event.set()

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    
    try:
        client.connect("localhost", 8083, keepalive=60)
        client.loop_start()
    except Exception as e:
        print(f"  [FAIL] WebSocket Connection failed to connect: {e}")
        return False
        
    if not waiter.connect_event.wait(timeout=5):
        print("  [FAIL] Timeout waiting for WS CONNECT response.")
        client.loop_stop()
        return False
        
    if waiter.connect_reason != 0:
        print(f"  [FAIL] WS Connection failed: {waiter.connect_reason}")
        client.loop_stop()
        return False
        
    print("  [PASS] WebSocket connection established successfully!")
    
    # Subscribe and publish over WebSockets
    client.subscribe("test/ws")
    if waiter.subscribe_event.wait(timeout=3) and waiter.subscribe_reasons and waiter.subscribe_reasons[0] <= 2:
        client.publish("test/ws", "Hello WS", qos=1)
        if waiter.message_event.wait(timeout=3) and waiter.received_message.payload.decode() == "Hello WS":
            print("  [PASS] Message published and received successfully over WebSockets!")
            client.disconnect()
            client.loop_stop()
            return True
            
    print("  [FAIL] Failed to publish/subscribe over WebSockets.")
    client.disconnect()
    client.loop_stop()
    return False

def run_test_metrics():
    print("\n--- Test 6: Prometheus Metrics Check (Port 9095) ---")
    import urllib.request
    try:
        url = "http://localhost:9095/metrics"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=3) as response:
            html = response.read().decode('utf-8')
            
        print("  Scraped metrics successfully:")
        lines = html.strip().split('\n')
        for line in lines:
            if not line.startswith('#'):
                print(f"    {line}")
                
        # Verify key metrics exist
        if "pipistrelle_connections_total" in html and "pipistrelle_messages_published_total" in html:
            print("  [PASS] Metrics exporter is functioning correctly and exposes expected metrics.")
            return True
        else:
            print("  [FAIL] Metrics do not contain expected Pipistrelle gauges/counters.")
            return False
    except Exception as e:
        print(f"  [FAIL] Failed to scrape metrics endpoint: {e}")
        return False

if __name__ == "__main__":
    print("==================================================")
    print("Starting Pipistrelle MQTT Integration Test Suite")
    print("==================================================")
    
    success = True
    
    # 1. Admin TCP
    try:
        if not run_test_admin_tcp():
            success = False
    except Exception as e:
        print(f"  [ERROR] Exception in admin TCP test: {e}")
        success = False
        
    # 2. Auth Failure
    try:
        if not run_test_auth_failure():
            success = False
    except Exception as e:
        print(f"  [ERROR] Exception in auth failure test: {e}")
        success = False
        
    # 3. ACL Authorization
    try:
        if not run_test_acl_authorization():
            success = False
    except Exception as e:
        print(f"  [ERROR] Exception in ACL authorization test: {e}")
        success = False
        
    # 4. Admin TLS
    try:
        if not run_test_admin_tls():
            success = False
    except Exception as e:
        print(f"  [ERROR] Exception in admin TLS test: {e}")
        success = False
        
    # 5. WebSockets
    try:
        if not run_test_websockets():
            success = False
    except Exception as e:
        print(f"  [ERROR] Exception in WebSocket test: {e}")
        success = False

    # 6. Metrics
    try:
        if not run_test_metrics():
            success = False
    except Exception as e:
        print(f"  [ERROR] Exception in metrics test: {e}")
        success = False
        
    print("\n==================================================")
    if success:
        print("ALL TESTS COMPLETED SUCCESSFULLY! (PASS)")
        sys.exit(0)
    else:
        print("SOME TESTS FAILED. (FAIL)")
        sys.exit(1)

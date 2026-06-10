import sys
import paho.mqtt.client as mqtt

# Usamos la API v2 de paho-mqtt
CallbackAPI = mqtt.CallbackAPIVersion.VERSION2

def main():
    if len(sys.argv) < 2:
        print("Uso: python send_alert.py \"Tu mensaje de alerta aqui\"")
        sys.exit(1)
        
    mensaje = sys.argv[1]
    topic = "alerts/critical"
    
    print(f"Conectando al Broker local en localhost:1883...")
    client = mqtt.Client(callback_api_version=CallbackAPI, protocol=mqtt.MQTTv5)
    client.username_pw_set("admin", "admin123")
    
    try:
        client.connect("localhost", 1883, keepalive=60)
        print(f"Publicando alerta: '{mensaje}' en el topico '{topic}'...")
        client.publish(topic, mensaje, qos=1)
        client.disconnect()
        print("¡Mensaje de alerta enviado con éxito!")
    except Exception as e:
        print(f"Error al enviar mensaje: {e}")

if __name__ == "__main__":
    main()

from flask import Flask, jsonify, request, Response
from datetime import datetime
import logging
import configparser
import json


from azure.eventhub import EventHubProducerClient
from azure.eventhub import EventData
from azure.identity import DefaultAzureCredential

app = Flask(__name__)

config = configparser.ConfigParser()
config.read('config.ini')

log_path = config.get('logs', 'we_path')
https_auth = config.get('this', 'auth')
CONNECTION_STR = str(config.get('event-hub', 'CONNECTION_STR_SEND')).replace("'","")
EVENTHUB_NAME = config.get('event-hub', 'EVENTHUB_NAME')
print(EVENTHUB_NAME, CONNECTION_STR)
logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
credential = DefaultAzureCredential()


def send_payload_to_eh(payload, category):
    full_message = {"category": category, "payload" : payload}
    client = client = EventHubProducerClient.from_connection_string(conn_str=CONNECTION_STR, eventhub_name=EVENTHUB_NAME)
    event_data_batch = client.create_batch()
    event_data_batch.add(EventData(json.dumps(full_message).encode('utf-8')))
    print(payload)
    client.send_batch(event_data_batch)




app.run(host='20.24.19.71', port=5000, debug=True)

@app.route("/license_data", methods=['POST'])
def receive_license_payloads():
    
    auth = request.headers.get('Authorization')
    if not auth:
        return Response(
            "Authentication error: Authorization header is missing",
            status=401
        )
    parts = auth.split()

    if parts[0].lower() != "bearer":
        return Response("Authentication error: Authorization header must start with ' Bearer'", status=401)
    elif len(parts) == 1:
        return Response("Authentication error: Token not found", status=401)
    elif len(parts) > 2:
        return Response("Authentication error: Authorization header must be 'Bearer <token>'", status=401)
    
    elif auth != https_auth:
        return Response("Authentication error: Wrong Authorization", status=401)
    # payload = request.get_json()
    # send_payload_to_eh(payload)   
    
    try:
        payload = request.get_json()
        send_payload_to_eh(payload, "license_data")
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    
    return Response("Payload processed successfully.", status=200)

@app.route("/up-noah_data", methods=['POST'])
def receive_upnoah_payloads():
    
    auth = request.headers.get('Authorization')
    if not auth:
        return Response(
            "Authentication error: Authorization header is missing",
            status=401
        )
    parts = auth.split()

    if parts[0].lower() != "bearer":
        return Response("Authentication error: Authorization header must start with ' Bearer'", status=401)
    elif len(parts) == 1:
        return Response("Authentication error: Token not found", status=401)
    elif len(parts) > 2:
        return Response("Authentication error: Authorization header must be 'Bearer <token>'", status=401)
    
    elif auth != https_auth:
        return Response("Authentication error: Wrong Authorization", status=401)
    # payload = request.get_json()
    # send_payload_to_eh(payload)   
    
    try:
        
        payload = request.get_json()
        send_payload_to_eh(payload, "up-noah_data")
    except Exception as e:
        logger.error(f"ERROR: {e}")
        return Response(f"ERROR: {e}", status=501)
    
    return Response("Payload processed successfully.", status=200)
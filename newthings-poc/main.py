import os
import json
import requests as req
import redis
from datetime import datetime, date, timedelta
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from requests.auth import HTTPBasicAuth
import ast


CONNECTION_STR = ''
EVENTHUB_NAME = ''
CONSUMER_GROUP = ''
BLOB_STORAGE_CONTAINER = ''
BLOB_STORAGE_CONNECTION_STRING = ''
REDIS_HOST = ''
REDIS_PORT = 6379
REDIS_PASS = ""
ERRORS_PATH = "ERRORS"
ARCH_PATH = "Archives"




def get_app_values(app_name):
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)
    r.ping()
    return r.get(app_name)

def send_request(URL, payload, header, filtr, UN, PW):
    try:
        res = req.post(URL, json=payload, headers=header, auth=HTTPBasicAuth(UN,  PW), params=filtr, timeout=5)
    except Exception as e:
        payload = str(payload).replace("'", '"')
        f = open(f"main.log", "a")
        f.write(f"\n{datetime.now()} - Error sending the following payload to {URL}: {payload}")
        f.close()

        f = open(f"{ERRORS_PATH}/request_err_payloads.json", "a")
        f.write(f"\n{payload}")
        f.close()

    else:
        f = open(f"main.log", "a")
        f.write(f"\n{datetime.now()} - Successfully sent the following payload to {URL}: {payload}")
        f.close()

        if checkIfMidnight():
            LOG_FILE_NAME = f"main_{date.today() - timedelta(days = 1)}.log"
            if not os.path.isfile(f"{ARCH_PATH}/{LOG_FILE_NAME}.gz"):
                os.system(f"mv main.log {ARCH_PATH}/{LOG_FILE_NAME}")
                os.system(f"gzip -9 {ARCH_PATH}/{LOG_FILE_NAME}")


def checkIfMidnight():
    now = datetime.now()
    seconds_since_midnight = (now - now.replace(hour=0, minute=0, second=0, microsecond=0)).total_seconds()
    return seconds_since_midnight >= 0

def process_data(payload):
    r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, password=REDIS_PASS)
    for key in r.keys():
        val = str(get_app_values(key).decode("utf-8")).replace("'", '"')
        webhooks = json.loads(val)
        if webhooks != None:
            for url in webhooks.keys():
                header = webhooks[url]["additional_headers"] 
                filtr = webhooks[url]["filter_event_data"] 
                un = webhooks[url]["basic_auth_un"]
                pw = webhooks[url]["basic_auth_pw"]
                send_request(url, payload, header, filtr, un, pw)
                
    
    # app_name = payload["end_device_ids"]["application_ids"]["application_id"]
    # app_target = get_app_values(app_name)

    # if app_target != None:
    #     app_target = json.loads(app_target)
    #     for key in app_target:
    #         URL = key
    #         header  = app_target[key]
    #         send_request(URL, payload, header)
    # else:
    #     # f = open(f"{ERRORS_PATH}/app_not_exist_payload.json", "a")
    #     # f.write(f"\n{payload}")
    #     # f.close()
    #     pass


def on_event(partition_context, event):
    data = json.loads(event.body_as_str(encoding='UTF-8'))
    # try:
    process_data(data["payload"])
        #print("\n",data)
    # except Exception as e:
    #     print(e)
    # else:
    partition_context.update_checkpoint(event)

def on_partition_initialize(partition_context):
    # Put your code here.
    print("Partition: {} has been initialized.".format(partition_context.partition_id))
    f = open(f"main.log", "a")
    f.write(f"\n{datetime.now()} - Partition: {partition_context.partition_id} has been initialized.")
    f.close()


def on_partition_close(partition_context, reason):
    # Put your code here.
    print("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))
    f = open(f"main.log", "a")
    f.write(f"\n{datetime.now()} - Partition: {partition_context.partition_id} has been closed, reason for closing: {reason}.")
    f.close()  


def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
        f = open(f"main.log", "a")
        f.write(f"\n{datetime.now()} - An exception: {partition_context.partition_id} occurred during receiving from Partition: {error}.")
        f.close() 
    else:
        print("An exception: {} occurred during the load balance process.".format(error))
        f = open(f"main.log", "a")
        f.write(f"\n{datetime.now()} - An exception: {error} occurred during the load balance process.")
        f.close() 
        
        
if __name__ == '__main__':
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_STORAGE_CONNECTION_STRING,
        BLOB_STORAGE_CONTAINER
    )
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store
    )

    try:
        with consumer_client:
            consumer_client.receive(
                on_event=on_event,
                on_partition_initialize=on_partition_initialize,
                on_partition_close=on_partition_close,
                on_error=on_error,
                starting_position="-1",  # "-1" is from the beginning of the partition.
            )
    except KeyboardInterrupt:
        print('Stopped receiving.')
        f = open(f"main.log", "a")
        f.write(f"\n{datetime.now()} - Stopped receiving.")
        f.close() 
        

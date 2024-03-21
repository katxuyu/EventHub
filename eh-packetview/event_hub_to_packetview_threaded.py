#!/usr/bin/env python

import os
import json
import requests 
from datetime import datetime
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
from multiprocessing.pool import Pool


CONNECTION_STR = 'Endpoint=sb://pwxpayloads1.servicebus.windows.net/;SharedAccessKeyName=packetviewread;SharedAccessKey=iZrEvrt+y8U0qtiAKBBD6zM0a0K/ZFfyB+AEhAJZXU0=;EntityPath=packetview'
EVENTHUB_NAME = 'packetview'
CONSUMER_GROUP = 'pvazurecg'
BLOB_STORAGE_CONTAINER = 'packetviewcheckpointstoreazure'
BLOB_STORAGE_CONNECTION_STRING = 'DefaultEndpointsProtocol=https;AccountName=pwxehcheckpointstore;AccountKey=8RXnBgPj0T7EbEpMwT2BEVRcYPAESSt4bdsbHQxGuxEmg1EOeGYPX/Umjbo/Nd7WvvR8yC94nc48+AStbVjWcQ==;EndpointSuffix=core.windows.net'

packetview_url='http://localhost:8080/api/v1/integrations/http/'
max_batch_size = 12

def send_messages(data):
    
    #    print(data)
    try:
        payload = data["payload"]
        key = data['key']
        ts = payload["uplink_message"]["received_at"]
#        print(key)

        final_url = packetview_url + key
        result = requests.post(final_url, json=payload)
        print(ts, final_url, result.status_code)

    except Exception as e:
        print("Error encountered see errors.txt")
        with open("errors.txt", "a") as file:
            file.write(json.dumps(payload)+"\n")
            file.write(f"{e}\n")
        pass
        #exit(1)

    


def on_event(partition_context, event_batch):
    # Put your code here.
    # If the operation is i/o intensive, multi-thread will have better performance.
    #print("Received event from partition: {}.".format(partition_context.partition_id))
    #print(event.body_as_str(encoding='UTF-8'))

    
    cpu_count = os.cpu_count() - 1
    params = []

    for event in event_batch:
        data = json.loads(event.body_as_str(encoding='UTF-8'))
        params.append(data)

    with Pool(processes=cpu_count) as pool:
        results = pool.map(send_messages, params)
        [result.wait() for result in results]
    partition_context.update_checkpoint()


def on_partition_initialize(partition_context):
    # Put your code here.
    print("Partition: {} has been initialized.".format(partition_context.partition_id))


def on_partition_close(partition_context, reason):
    # Put your code here.
    print("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))


def on_error(partition_context, error):
    # Put your code here. partition_context can be None in the on_error callback.
    if partition_context:
        print("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))


if __name__ == '__main__':
    print("Configuring checkpoint store")
    checkpoint_store = BlobCheckpointStore.from_connection_string(
        BLOB_STORAGE_CONNECTION_STRING,
        BLOB_STORAGE_CONTAINER
    )

    print("Initializing client")
    consumer_client = EventHubConsumerClient.from_connection_string(
        conn_str=CONNECTION_STR,
        consumer_group=CONSUMER_GROUP,
        eventhub_name=EVENTHUB_NAME,
        checkpoint_store=checkpoint_store
    )

    print("Waiting for events")
    try:
        with consumer_client:
            consumer_client.receive_batch(
                on_event_batch=on_event,
                on_partition_initialize=on_partition_initialize,
                max_batch_size = max_batch_size,
                on_partition_close=on_partition_close,
                on_error=on_error,
                starting_position="-1",  # "-1" is from the beginning of the partition.
            )
    except KeyboardInterrupt:
        print('Stopped receiving.')

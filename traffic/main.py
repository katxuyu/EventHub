import json
import requests as req
from azure.eventhub import EventHubConsumerClient
from azure.eventhub.extensions.checkpointstoreblob import BlobCheckpointStore
import logging
import time
import csv

import configparser

config = configparser.ConfigParser(interpolation=None)
config.read('config.ini')

CONNECTION_STR = str(config.get('event-hub', 'CONNECTION_STR_LISTEN')).replace("'","")
EVENTHUB_NAME = config.get('event-hub', 'EVENTHUB_NAME')
CONSUMER_GROUP = config.get('event-hub', 'CONSUMER_GROUP')
BLOB_STORAGE_CONTAINER = config.get('event-hub', 'BLOB_STORAGE_CONTAINER')
BLOB_STORAGE_CONNECTION_STRING = str(config.get('event-hub', 'BLOB_STORAGE_CONNECTION_STRING', raw=True)).replace("'","")
print(BLOB_STORAGE_CONNECTION_STRING)
target = json.loads(config.get('targets', 'URL'))
data_path = config.get('data', 'path')
log_path = config.get('logs', 'path')


logging.basicConfig(filename=log_path,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dataset = []

def process_data(payload):
        global dataset
    #try:
        if len(dataset) == 50:
            with open(f"{data_path}/ltr-{time.strftime('%Y%m%d-%H:%M:%S')}.json", "a") as f:
                csv.writer(f, delimiter="\n").writerow(dataset)
            dataset = []
        else:
            dataset.append(payload)
        c_payload  = json.loads(str(payload).replace("'", '"'))
        for t in target:
            print(t, (c_payload["category"] == t))
            if c_payload["category"] == t:
                p = c_payload["payload"]
                URL = target[t][0]
                
                AUTH = target[t][1] if len(target[t]) > 1 else ""
                print(p, URL, AUTH)
                header = {'Authorization': f'{AUTH}'}
                res = req.post(URL, json=p, headers=header)
                print(res, p)
                break
        
    # except Exception as e:
    #     logger.error(f"{e}")


def on_event(partition_context, event):
    data = json.loads(event.body_as_str(encoding='UTF-8'))
    # try:
    print(data, data["payload"])
    process_data(data)
        #print("\n",data)
    # except Exception as e:
    #     print(e)
    # else:
    partition_context.update_checkpoint(event)

def on_partition_initialize(partition_context):
    # Put your code here.
    print("Partition: {} has been initialized.".format(partition_context.partition_id))
    logging.info("Partition: {} has been initialized.".format(partition_context.partition_id))


def on_partition_close(partition_context, reason):
    # Put your code here.
    print("Partition: {} has been closed, reason for closing: {}.".format(
        partition_context.partition_id,
        reason
    ))
    logging.error("Partition: {} has been closed, reason for closing: {}.".format(
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
        logging.error("An exception: {} occurred during receiving from Partition: {}.".format(
            partition_context.partition_id,
            error
        ))
    else:
        print("An exception: {} occurred during the load balance process.".format(error))
        logging.error("An exception: {} occurred during the load balance process.".format(error))
        
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
        logging.warning("Stopped Receiving.")
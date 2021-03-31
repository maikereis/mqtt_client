import traceback
import logging
import time
import paho.mqtt.client as mqtt
import os
import datetime
import json
import re
from mysql_connector import open_connection, create_database, create_table, select_database, insert_data, insert_payload

import requests
url = "http://www.google.com.br"
timeout = 5
try:
 	request = requests.get(url, timeout=timeout)
 	print("Connected to the Internet")
except (requests.ConnectionError, requests.Timeout) as exception:
    print("No internet connection.")


dir_path = os.path.dirname(os.path.realpath(__file__))


with open(dir_path + '/cfg_files/'+'database_cfg.json', 'r') as db_file:
    db_cfg = json.load(db_file)


DB_NAME = db_cfg['DB_NAME']
TB_NAME = db_cfg['PERSIST_TB_NAME']


# Define the callback to handle CONNACK from the broker, if the connection created normal, the value of rc is 0
def on_connect(client, userdata, flags, rc):
    print("On_connect:" + str(rc))

    if(str(rc) == '0'):
        print("connected")
        client.subscribe(topic='/#', qos=2)

   


# Define the callback to hande publish from broker, here we simply print out the topic and payload of the received message
def on_message(client, userdata, msg):
    cursor = open_connection(DB_NAME, db_cfg)
    select_database(cursor, DB_NAME)
    create_table(cursor, TB_NAME)

    print("New message\n Topic: {}".format(msg.topic))
    json_obj_lst = json.loads(msg.payload)
    payload_lst = []
    for obj in json_obj_lst:
        #print(" Payload: {}\n".format(obj), sep="")
        for key in list(obj.keys()):
            if((key != 'ID') & (key != 'D') & (key != 'S')):
                try:     
                    data = {'client_id': obj['ID'],
                        'payload': obj[key],
                        'topic_path': msg.topic+'/'+ obj['S']+'/'+key,
                        'date': datetime.datetime.fromtimestamp(int(obj['D']))}
                    payload_lst.append(data)
                except:
                    print("Error")

    insert_payload(cursor, TB_NAME, payload_lst)

# Callback handles disconnection, print the rc value
def on_disconnect(client, userdata, rc):
    print("On_disconnect:" + str(rc))


def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(client) + " " + str(userdata))


# Get listener client Login/Pass
with open(dir_path + '/cfg_files/'+'client_cfg.json', 'r') as cli_file:
    cli_cfg = json.load(cli_file)


PASS = cli_cfg['PASS']
USER = cli_cfg['USER']
HOST = cli_cfg['HOST']
PORT = cli_cfg['PORT']
KPAL = cli_cfg['KEEP_ALIVE']


# Create an instance of `Client`
client = mqtt.Client(client_id="PersistScript", clean_session=False)
client.username_pw_set(username=USER, password=PASS)

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.on_subscribe = on_subscribe

# Connect to broker
# connect() is blocking, it returns when the connection is successful or failed. If you want client connects in a non-blocking way, you may use connect_async() instead
while True:
    try:
        client.connect(HOST, PORT, 240)
    except Exception as e:
        print(e)
        time.sleep(1)
    else:
        break


client.loop_forever()
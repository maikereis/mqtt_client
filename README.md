# Python MQTT client

This repository is part of my undergraduate monograph. The objectives of the work were:

Data collection using IoT nodes Analyze the consumption of electricity in a home and correlate them with environmental variables The data collection was made using IoT nodes based on ESP32 ([Node program](https://github.com/XxKavosxX/iot_node)), and a Python MQTT client to persist data in a AWS database.

The data collected was cleaned and analysed using python scripts and jupyter notebooks presented in [this](https://github.com/XxKavosxX/consumption_data_analysis)  repository.
  
This is a simple MQTT client to store data on MySQL/MariaDB databases
 
The project was designed and tested to work with EMQX-broker and MariaDB on AWS
feel free to use and modify it.
  
Are two modules here:
  - client.py : who connect to EMQX-broker and wait for messages
  - mysql_connector.py : who implement the functions needed by connect database, store data and retrieve data.

You need to add your database configurations under cfg_files/database_cfg.json
And the mqtt client configurations under cfg_files/client_cfg.json


  THIS SIMPLE MQTT CLIENT TO STORE DATA ON MySQL/MariaDB DATABASES
 
  The project was designed and tested to work with EMQX-broker and MariaDB on AWS
  feel free to use and modify it.
  
  Are two modules here:
  - client.py : who connect to EMQX-broker and wait for messages
  - mysql_connector.py : who implement the functions needed by connect database, store data and retrieve data.

  You need to add your database configurations under cfg_files/database_cfg.json
  And the mqtt client configurations under cfg_files/client_cfg.json


#!/usr/bin/env python
# coding: utf-8

import mysql.connector
from mysql.connector import errorcode
from datetime import date, datetime, timedelta
import json
import os


TABLES = {}
TABLES['emqx_broker_topics'] = (
    "CREATE TABLE `emqx_broker_topics` ("
    "  `id` int(11) NOT NULL AUTO_INCREMENT,"
    "  `client_id` varchar(30) NOT NULL,"
    "  `payload` varchar(60),"
    "  `topic_path` varchar(60) NOT NULL,"
    # "  `will_message` varchar(20),"
    # "  `qos` varchar(2) NOT NULL,"
    "  `date` datetime NOT NULL,"
    "  PRIMARY KEY (`id`)"
    ") DEFAULT CHARSET=utf8,"
    "  ENGINE=InnoDB")

TABLES['mqtt_user'] = (
    "CREATE TABLE `mqtt_user` ("
    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
    "`username` varchar(100) DEFAULT NULL,"
    "`password` varchar(100) DEFAULT NULL,"
    "`salt` varchar(35) DEFAULT NULL,"
    "`is_superuser` tinyint(1) DEFAULT 0,"
    "`created` datetime DEFAULT NULL,"
    "PRIMARY KEY (`id`),"
    "UNIQUE KEY `mqtt_username` (`username`)"
    ") ENGINE=MyISAM DEFAULT CHARSET=utf8;"
)

TABLES['mqtt_acl'] = (
    "CREATE TABLE `mqtt_acl` ("
    "`id` int(11) unsigned NOT NULL AUTO_INCREMENT,"
    "`allow` int(1) DEFAULT NULL COMMENT '0: deny, 1: allow',"
    "`ipaddr` varchar(60) DEFAULT NULL COMMENT 'IpAddress',"
    "`username` varchar(100) DEFAULT NULL COMMENT 'Username',"
    "`clientid` varchar(100) DEFAULT NULL COMMENT 'ClientId',"
    "`access` int(2) NOT NULL COMMENT '1: subscribe, 2: publish, 3: pubsub',"
    "`topic` varchar(100) NOT NULL DEFAULT '' COMMENT 'Topic Filter',"
    "PRIMARY KEY (`id`)"
    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
)


def open_connection(DB_NAME, cfg):
    #print('OPEN_CONNECTION')
    try:
        cnx = mysql.connector.connect(user=cfg['BROKER_USER'], password=cfg['BROKER_PASS'], host=cfg['HOST'], port=cfg['PORT'],
                                      database=DB_NAME)
        #print("Success to connect database: {}.".format(DB_NAME))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    return cnx


def create_database(cnx, DB_NAME):
    #print('CREATE_DATABASE')
    try:
        cnx.cursor().execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}.".format(err))
        return


def create_table(cnx, TB_NAME):
    #print('CREATE_TABLE')
    print("On table: {} ".format(TB_NAME))

    query = TABLES[TB_NAME]
    try:
        #print("Creating table: {} ".format(TB_NAME), end='')
        cnx.cursor().execute(query)
    except mysql.connector.Error as err:
        if(err.errno != errorcode.ER_TABLE_EXISTS_ERROR):
            print(err.msg)
    else:
        print("table created!")


def select_database(cnx, DB_NAME):
    #print('SELECT_DATABASE')
    try:
        cnx.cursor().execute("USE {}".format(DB_NAME))
        print("Using database: {} ".format(DB_NAME))
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cnx.cursor(), DB_NAME)
            print("Database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            return

def insert_payload(cnx, TB_NAME, data_lst):
    if(TB_NAME == 'emqx_broker_topics'):
        query = ("INSERT INTO emqx_broker_topics "
                 "(client_id, payload, topic_path, date) "
                 "VALUES (%(client_id)s, %(payload)s, %(topic_path)s, %(date)s)")

    print("Try inserting {} entries on Database".format(len(data_lst)))

    try:
        cnx.cursor().executemany(query, data_lst)
        print("Data was entered! ")
        cnx.commit()
    except mysql.connector.Error as err:
        print("Failed inserting data: {}.".format(err))
        return

def insert_data(cnx, TB_NAME, data):
    #print('INSERT_DATA')
    if(TB_NAME == 'emqx_broker_topics'):
        query = ("INSERT INTO emqx_broker_topics "
                 "(client_id, payload, topic_path, date) "
                 "VALUES (%(client_id)s, %(payload)s, %(topic_path)s, %(date)s)")
    elif(TB_NAME == 'mqtt_user'):
        query = ("INSERT INTO mqtt_user "
                 "(id, username, password, salt, is_superuser, created) "
                 "VALUES (%(id)s, %(username)s, %(password)s, %(salt)s,%(is_superuser)s, %(created)s)")
    elif(TB_NAME == 'mqtt_acl'):
        query = ("INSERT INTO mqtt_acl "
                 "(id, allow, ipaddr, username, clientid, access, topic) "
                 "VALUES (%(id)s, %(allow)s, %(ipaddr)s, %(username)s,%(clientid)s, %(access)s, %(topic)s)")
    else:
        print('NOT VALID TABLE')
    try:
        cnx.cursor().execute(query, data)
        cnx.commit()
        print("Data was entered! ")
    except mysql.connector.Error as err:
        print("Failed inserting data: {}.".format(err))
        return


def select_data(cnx, TB_NAME):
    c = cnx.cursor()
    c.execute("SELECT * FROM {}".format(TB_NAME))
    rows = c.fetchall()
    return rows
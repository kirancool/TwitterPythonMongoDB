# -*- coding: utf-8 -*-
"""
Created on Thu Mar 24 10:06:46 2022

@author: Kiran Adhav
"""

from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads


consumer = KafkaConsumer(
    'test',
     bootstrap_servers=['localhost:9092'],
     auto_offset_reset='earliest',
     enable_auto_commit=True,
     group_id='my-group',
     value_deserializer=lambda x: loads(x.decode('utf-8')))

client = MongoClient('localhost:27017')
collection = client.test_data.test_tweet

for message in consumer:
    message = message.value
    collection.insert_one(message)
    print(message)
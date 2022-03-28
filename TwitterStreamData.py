# -*- coding: utf-8 -*-
"""
Created on Mon Mar 28 12:18:20 2022

@author: Kiran Adhav
"""

import pymongo
from pymongo import MongoClient
import tweepy
import json
import pandas as pd


auth = tweepy.OAuthHandler("", "")
auth.set_access_token("", "")
api = tweepy.API(auth,wait_on_rate_limit=True)

client=MongoClient()
db=client.tweet_db
tweet_collection=db.tweet_collection
tweet_collection.create_index([("id",pymongo.ASCENDING)],unique=True)
MAX_TWEETS = 50000
df=tweepy.Cursor(api.search_tweets, q='#iphone12').items(MAX_TWEETS)
dff=[]
for tweet in df:
    text = tweet._json
    tweet_collection.insert_one(text)
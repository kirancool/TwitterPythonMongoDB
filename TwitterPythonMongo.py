from tweepy.models import Status
import pymongo
from pymongo import MongoClient
import tweepy
import json
import pandas as pd


auth = tweepy.OAuthHandler("pass api key")
auth.set_access_token("pass token key")
api = tweepy.API(auth,wait_on_rate_limit=True)

client=MongoClient()
db=client.tweet_db
tweet_collection=db.tweet_collection
tweet_collection.create_index([("id",pymongo.ASCENDING)],unique=True)
MAX_TWEETS = 50
df=tweepy.Cursor(api.search_tweets, q='#python').items(MAX_TWEETS)
dff=[]
for tweet in df:
    text = tweet._json
   #tweet_collection.insert_one(text)
    dff1={'contributors':text['contributors'],
  'coordinates':text['coordinates'],
  'created_at':text['created_at'],
                'id':text['id'],
                'id_str':text['id_str'],
                'text':text['text']}
  #text.items()
    dff.append(dff1)
    strdata=json.dumps(text)
a=pd.DataFrame(dff)
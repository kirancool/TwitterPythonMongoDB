
import tweepy
from time import sleep
from json import dumps
from kafka import KafkaProducer

topic_name = "test"   
auth = tweepy.OAuthHandler("")
auth.set_access_token("")
api = tweepy.API(auth,wait_on_rate_limit=True)

MAX_TWEETS=500


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],value_serializer=lambda x: dumps(x).encode('utf-8')) 
df=tweepy.Cursor(api.search_tweets, q='#python').items(MAX_TWEETS)

for tweet in df:
    text=tweet._json
    producer.send(topic_name,text)
    #print(producer.send('test',text))
    sleep(5)
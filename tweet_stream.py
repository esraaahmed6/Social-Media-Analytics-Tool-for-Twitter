#!/usr/bin/python3 

from kafka import KafkaProducer
from datetime import datetime
import tweepy
import sys
import re

TWEET_TOPICS = ['covid19']

KAFKA_BROKER = '172.18.0.2:6667'
KAFKA_TOPIC = 'test1'

class Streamer(tweepy.StreamListener):

    def on_error(self, status_code):
        if status_code == 402:
            return False

    def on_status(self, status):
        tweet = status.text

        tweet = re.sub(r'RT\s@\w*:\s', '', tweet)
        tweet = re.sub(r'https?.*', '', tweet)

        global producer
        producer.send(KAFKA_TOPIC, bytes(tweet, encoding='utf-8'))

        d = datetime.now()

        print(f'[{d.hour}:{d.minute}.{d.second}] sending tweet')

# put API keys here
consumer_key = "wHGi2BtgLE5cGSn3AmAWl4QPd"
consumer_secret_key = "gPIyjlqhRX7Fh70l973n1qZxG7WJBWnx98sW2RKYgWrI04ogFp"

access_token = "1385679347428872192-DVdTE8zU7IvAhD57sD3eGkh8Wl2vtG"
access_token_secret = "lnOnD4q6545sSb01QcMDUxaYQJtRfF5Hh5rILcgOmKIGS"


auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

streamer = Streamer()
stream = tweepy.Stream(auth=api.auth, listener=streamer)

try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
except Exception as e:
    print(f'Error Connecting to Kafka --> {e}')
    sys.exit(1)

stream.filter(track=TWEET_TOPICS)

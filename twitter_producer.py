#!/usr/bin/python3 

from kafka import KafkaConsumer , KafkaProducer
from datetime import datetime
import sys
import tweepy 
import time

KAFKA_BROKER = '172.18.0.2:6667'
KAFKA_TOPIC = 'test1'

#Put API key here 
consumer_key = "HgMvQsFRLSCnNsWZKzAWCC8sa"
consumer_secret_key = "aNc1QlRFWN0jpG5DWCjh6ib5KRPBMOI3uQYEKMH4kPH4JdzilS"
access_token = "1385696117938077700-2jxnaoZYujojv9kS7Se1Pegkjfr7Vj"
access_token_secret = "74KlJAkFvCdQJ5MclE8xUbzadtweTPfnmHE8H6MXdkoaP" 
auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)
    
#trying to connect with kafka        
try:
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER)
except Exception as e:
    print(f'Error Connecting to Kafka --> {e}')
    sys.exit(1)

# convert timestamp to normal time
def timestamp_clean(time):
    mytime = datetime.strptime(time, "%Y-%m-%d %H:%M:%S")
    return (mytime.strftime("%Y-%m-%d %H:%M:%S"))
    

#fetch data from twitter
def fetch_data():
    res = api.search("covid19")
    for i in res:
        record =''
        record +=str(i.user.id_str)
        record += ';'
        record += str(timestamp_clean(str(i.created_at)))
        record += ';'
        record +=str(i.user.followers_count)
        record += ';'
        record +=str(i.user.location)
        record += ';'
        record +=str(i.text)
        record += ';'
        record +=str(i.retweet_count)
        record += ';'
        record +=str(i.id)
        record += ';'
        record +=str(i.user.screen_name)
        record += ';'
        producer.send(KAFKA_TOPIC, str.encode(record))
        
        
    #Print time Send tweets
    d = datetime.now()
    print(f'[{d.hour}:{d.minute}.{d.second}] sending tweet')  
    
#time that required to fetch data   
def time_fetch(interval):
    while True:
        fetch_data()
        time.sleep(interval)
        
time_fetch(60* 0.1)

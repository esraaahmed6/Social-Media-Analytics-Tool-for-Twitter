from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from textblob import TextBlob
from datetime import datetime
import time
import tweepy 


#clean data
def clean(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()    
    words= words.withColumn('user_id',f.split('word',';').getItem(0)).withColumn('timestamp_time', f.split('word', ';').getItem(1)).withColumn('followers_count', f.split('word', ';').getItem(2)).withColumn('location', f.split('word', ';').getItem(3)).withColumn('tweet', f.split('word', ';').getItem(4)).withColumn('retweet_count', f.split('word', ';').getItem(5)).withColumn('tweet_id', f.split('word', ';').getItem(6)).withColumn('user_name', f.split('word', ';').getItem(7)) 
    words.printSchema()
    return words

# text classification
def polarity_detection(text):
    return TextBlob(text).sentiment.polarity
def subjectivity_detection(text):
    return TextBlob(text).sentiment.subjectivity
def text_classification(words):
    # polarity detection
    polarity_detection_udf = udf(polarity_detection, StringType())
    words = words.withColumn("polarity", polarity_detection_udf("word"))
    # subjectivity detection
    subjectivity_detection_udf = udf(subjectivity_detection, StringType())
    words = words.withColumn("subjectivity", subjectivity_detection_udf("word"))
    return words
    
    
# reply to tweets method    
    
def reply_to_tweet(tweet):
    user = tweet.user_name
    if tweet.sentiment=="positive":
      msg = "@%s Thank you for spreading the positivity!" %user
    else:
      msg = "@%s sorry for this feeling !" %user
    msg_sent = api.update_status(msg, tweet.tweet_id)
    print("--Reply Posted--")
    
    
if __name__ == "__main__":

    #reply on tweets
    consumer_key = "HgMvQsFRLSCnNsWZKzAWCC8sa"
    consumer_secret_key = "aNc1QlRFWN0jpG5DWCjh6ib5KRPBMOI3uQYEKMH4kPH4JdzilS"
    access_token = "1385696117938077700-2jxnaoZYujojv9kS7Se1Pegkjfr7Vj"
    access_token_secret = "74KlJAkFvCdQJ5MclE8xUbzadtweTPfnmHE8H6MXdkoaP"
 
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    
    # create Spark session
    spark = SparkSession.builder.appName("Twitter").getOrCreate()
    spark.conf.set("spark.sql.parquet.compression.codec", "gzip")
    
    #To avoid unncessary logs
    #spark.setLogLevel("WARN")
    
   
    
    # Subscribe to test1 topic 
    lines = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "172.18.0.2:6667").option("subscribe", "test1").load()
    lines.printSchema()
    
    # Preprocess the data
    words = clean(lines)
    
    # text classification to define polarity and subjectivity
    words = text_classification(words)    
    words = words.withColumn('sentiment',when( words.polarity >= 0, "positive").otherwise("negative"))
    words = words.repartition(1)   
    reply_query = words.writeStream.foreach(reply_to_tweet).start()
    
    #save files into hdfs    
    query = words.writeStream.queryName("all_tweets")\
        .outputMode("append").format("parquet")\
        .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/twitter.db/tweets")\
        .option("checkpointLocation","hdfs://sandbox-hdp.hortonworks.com:8020/apps/hive/warehouse/twitter.db/check")\
        .trigger(processingTime='60 seconds').start()
        
    query.awaitTermination()
    reply_query.awaitTermination()

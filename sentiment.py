from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as f
from textblob import TextBlob
from datetime import datetime
import time
import tweepy 


    
def clean(lines):
    words = lines.select(explode(split(lines.value, "t_end")).alias("word"))
    words = words.na.replace('', None)
    words = words.na.drop()
    words.printSchema()
    
    words= words.withColumn('user_id',f.split('word',';').getItem(0)).withColumn('timestamp', f.split('word', ';').getItem(1)).withColumn('followers_count', f.split('word', ';').getItem(2)).withColumn('location', f.split('word', ';').getItem(3)).withColumn('text', f.split('word', ';').getItem(4)).withColumn('retweet_count', f.split('word', ';').getItem(5)).withColumn('tweet_id', f.split('word', ';').getItem(6)).withColumn('user_name', f.split('word', ';').getItem(7))
    
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
      msg = "@%s Thanks for positivity" %user
    else:
      msg = "@%s sorry for that you're feeling that way!" %user
    msg_sent = api.update_status(msg, tweet.tweet_id)
    print("--Reply Posted--")
    
    
if __name__ == "__main__":

    #reply on tweets
    consumer_key = "xyRHPbiwuW9sWt1BSB86abNTp"
    consumer_secret_key = "6oynAT9ZdF3U5ZzizYaSxYF7EQ4Ntk9TSoA6AgTrTT9IDCYvaI"

    access_token = "1385679347428872192-TPO51sx9ZtmkjdgqhuKAgEvmWTuSUz"
    access_token_secret = "Dv27qMuj9BO2nZyvWxMiPLKMBmxOGeNOGDceG80J7anm6"

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret_key)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)
    
    # create Spark session
    spark = SparkSession.builder.appName("TwitterSentimentAnalysis").getOrCreate()
    #spark.conf.set("spark.sql.parquet.compression.codec", "gzip")

    
    # Subscribe to topic test1 
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
        .option("path", "hdfs://sandbox-hdp.hortonworks.com:8020/user/root/twitter")\
        .option("checkpointLocation","hdfs://sandbox-hdp.hortonworks.com:8020/user/root/twitter")\
        .trigger(processingTime='60 seconds').start()
    query.awaitTermination()
    reply_query.awaitTermination()
    
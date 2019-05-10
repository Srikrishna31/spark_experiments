'''
Code in this file is taken from the following sites:
https://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/
http://www.awesomestats.in/spark-twitter-stream/
http://mkuthan.github.io/blog/2015/03/01/spark-unit-testing/ - In memory DStream, RDD creation.
'''

from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
from collections import namedtuple
import datetime
import time
import matplotlib.pyplot  as plt
import json
import socket
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from threading import Thread

Tweet = namedtuple('Tweet', 'text user id createdAt retweetCount location language')

class MyListener(StreamListener):
    def __init__(self, socket):
        self.c_socket = socket

    def on_data(self, data):
        try:
            self.c_socket.send(str(data).encode('utf-8'))
            return True
        except BaseException as e:
            print("Error on_data: " + str(e))
        return True

    def on_error(self, status):
        print(status)
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False

def create_twitter_stream(consumerKey, consumerSecret, accessToken, accessTokenSecret):
    s= socket.socket()
    s.bind(("localhost", 8000))
    s.listen(5)
    auth = OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)
    c, _  = s.accept() #need to see if _ works.
    twitter_stream= Stream(auth, MyListener(c))
    #As per this link, we cant just filter by language with free access, so this is a workaround suggested.
    #https://stackoverflow.com/questions/26890605/filter-twitter-feeds-only-by-language
    twitter_stream.filter(track=['a'], languages=["en"])

def load_text(text):
    t = json.loads(text, encoding='utf-8')
    #print(t.to_bytes()
    return t

def tweet(t):
    print("****************************************" + str(type(t)))
    print(str(t).encode('utf-8'))
    assert(str(type(t) == "dict"))
    twt = Tweet(t['text'], t['user']['name'], t['id'], t['created_at'], t['retweet_count'], t['user']['location'], t['lang'])
    return twt

def rdd_to_tweet(text):
    js = json.loads(text, encoding='utf-8')
    t = tweet(js)
    return json.dumps(t, ensure_ascii=False)

def start_spark_streaming():
    sc = SparkContext("local[3]", "Spark-Python")
    ssc = StreamingContext(sc, 10)
    stream = ssc.socketTextStream("localhost", 8000)
    ssc.checkpoint("D:/TwitterStreamData/")

    #The RDD will be created every 10 seconds, but the data in RDD will be for the last 20 seconds.
    lines = stream.window(20)
    print(lines.count)
    lines.foreachRDD(lambda rdd: rdd.filter(rdd_to_tweet).coalesce(1).saveAsTextFile('D:/TwitterStreamData/' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S') + '.json'))
    #lines.flatMap(load_text)  \
    #     .map(tweet) \
    #     .saveAsTextFiles('D:/TwitterStreamData/' + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d_%H-%M-%S') + '.json')
         #.foreachRDD(lambda rdd: print(rdd))

    ssc.start() 
    ssc.awaitTermination()
   
if __name__=='__main__':
    import sys

    if len(sys.argv) < 5:
        print("Usage: Spark_Python.py 'consumerKey', 'consumerSecret', 'accessToken', 'accessTokenSecret' \n\
        Replace the consumerKey,secret etc with appropriate twitter hashes")
        sys.exit(1)

    t = Thread(target=create_twitter_stream, args=sys.argv[1:])
    t.setDaemon(True)
    t.start()

    start_spark_streaming()

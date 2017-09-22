'''
Code in this file is taken from the following sites:
https://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/
http://www.awesomestats.in/spark-twitter-stream/
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
            self.c_socket.send(json.loads(data).encode('utf-8'))
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
    twitter_stream= Stream(auth, MyListener(s))
    #As per this link, we cant just filter by language with free access, so this is a workaround suggested.
    #https://stackoverflow.com/questions/26890605/filter-twitter-feeds-only-by-language
    twitter_stream.filter(track=['a'], languages=["en"], async=True)


def start_spark_streaming():
    sc = SparkContext("local[3]", "Spark-Python")
    ssc = StreamingContext(sc, 10)
    stream = ssc.socketTextStream("localhost", 8000)
    ssc.checkpoint("D:/TwitterStreamData/")

    #The RDD will be created every 10 seconds, but the data in RDD will be for the last 20 seconds.
    lines = stream.window(20)
    lines.map(lambda text: json.loads(text))  \
         .map(lambda t: Tweet(t['text'], t['user'], t['id'], t['createdAt'], t['retweetCount'], t['location'], t['language']))
         #.forEachRDD(lambda rdd: rdd.)


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

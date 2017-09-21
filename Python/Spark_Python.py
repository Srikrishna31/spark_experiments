'''
Code in this file is taken from the following site:
https://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/
'''

from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
from threading import Thread
import multiprocessing
import collections
import datetime
import time
import matplotlib.pyplot  as plt


Tweet = collections.namedtuple('Tweet', 'text user id createdAt retweetCount location language')

class MYListener(StreamListener):
    def __init__(self):
        self.x = []
        self.y = []

    def on_data(self, data):
        try:
            plt.xlabel('python count')
            plt.ylabel('time stamp')
            plt.grid(True)
            iter = 0;
            with open('Python_1.json', 'a') as f:
                f.write(data)
                find_word = '#python'
                self.y.append(data.count(find_word))
                ts = time.time()
                st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
                self.x.append(st)
            # print (self.x, self.y)
            plt.xticks(range(0, len(self.x)), self.x)
            plt.bar(range(0, len(self.x)), self.y)
            plt.show()
            return True
        except BaseException as e:
            print("Error on_data: " + str(e))
        return True

    def on_error(self, status):
        print(status)
        return True


if __name__=='__main__':
    import sys

    if len(sys.argv) < 5:
        print("Usage: Spark_Python.py 'consumerKey', 'consumerSecret', 'accessToken', 'accessTokenSecret' \n\
        Replace the consumerKey,secret etc with appropriate twitter hashes")
        sys.exit(1)

    consumerKey, consumerSecret, accessToken, accessTokenSecret = \
                (sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])

    # thread = Thread(target=plot)
    # thread.setDaemon(True)
    # thread.start()
    auth = OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)

    twitter_stream= Stream(auth, MYListener())
    twitter_stream.filter(track=['#python'])

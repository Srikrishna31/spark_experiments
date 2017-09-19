'''
Code in this file is taken from the following site:
https://marcobonzanini.com/2015/03/02/mining-twitter-data-with-python-part-1/
'''

from tweepy import Stream, OAuthHandler
from tweepy.streaming import StreamListener
import collections
import datetime

Tweet = collections.namedtuple('Tweet', 'text user id createdAt retweetCount location language')

class MYListener(StreamListener):
    def on_data(self, data):
        try:
            #time = datetime.datetime()
            #time.
            with open(time.strftime('%Y%m%d-%H%M%S') + '.json', 'a') as f:
                f.write(data)
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

    auth = OAuthHandler(consumerKey, consumerSecret)
    auth.set_access_token(accessToken, accessTokenSecret)
    #auth.

    twitter_stream= Stream(auth, MYListener())
    twitter_stream.filter(track=['#python'])

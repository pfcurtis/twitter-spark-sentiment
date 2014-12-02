#!/bin/env python
import os
import sys
import SocketServer

sys.path.append('../lib')

from multiprocessing import Process, Queue
from tweepy import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
import json
import logging
from optparse import OptionParser
from HTMLParser import HTMLParser
import errno
from datetime import datetime
import ConfigParser
from flask import Flask,jsonify


rest_app = Flask(__name__)

this_dir = os.path.abspath(os.path.dirname(__file__))

logging.basicConfig()
logger = logging.getLogger(os.path.join(this_dir, 'twitter_stream'))
logger.setLevel(logging.INFO)

CONFIGFILE = '../conf/twitter_stream.config'
config = ConfigParser.ConfigParser()
config.read(CONFIGFILE)

try:
    CONSUMER_KEY = config.get('Twitter Keys', 'CONSUMER_KEY')
    CONSUMER_SECRET = config.get('Twitter Keys', 'CONSUMER_SECRET')
    ACCESS_TOKEN = config.get('Twitter Keys', 'ACCESS_TOKEN')
    ACCESS_TOKEN_SECRET = config.get('Twitter Keys', 'ACCESS_TOKEN_SECRET')
except ConfigParser.NoSectionError as e:
        logger.warn(e)
        sys.exit(1)



@rest_app.route('/query/<q_str>')
def change_twitter_query(q_str):
    global p2
    query_string = q_str
    p2.terminate()
    p2 = Process(target=run_tweet_capture, args=(query_string,))
    p2.start()
    return jsonify(query=query_string)


@rest_app.route('/query')
def return_twitter_query():
    return jsonify(query=query_string)


class DirNotFoundException(Exception): pass

class TweetSaver(object):
    """A utility to append tweets to a json file
        tweet_saver = TweetSaver(save_dir="/path/to/save/tweets")
        Will create the following file tree:
        <save_dir>/YYYY/MM/DD/HH/tweets.json
        based on the created_at field in the tweet.
    """
    def __init__(self, save_dir="."):
        self._saveDir = None
        self.saveDir = save_dir
        self._tweetCounter = 0
        self._twitter_time_format = "%a %b %d %H:%M:%S +0000 %Y"

    def _make_sure_path_exists(self, path):
        try:
            os.makedirs(path)
        except OSError as exception:
            if exception.errno != errno.EEXIST:
                raise

    @property
    def saveDir(self):
        return self._saveDir


    @saveDir.setter
    def saveDir(self, value):
        if not os.path.exists(value):
            raise DirNotFoundException("Directory %s not found!" % value)
        self._saveDir = value

    def saveTweet(self, tweet):
        """Appends tweet text (raw) to a tweets.json file in
        <self.saveDir>/YYYY/MM/DD/HH/tweets.json based on created_at field.
        """
        try:
            data = json.loads(HTMLParser().unescape(tweet))
            created_at = datetime.strptime(data['created_at'],
                                           self._twitter_time_format)
            save_dir = os.path.join(os.path.abspath(self._saveDir),
                                    str(created_at.year),
                                    str(created_at.month).zfill(2),
                                    str(created_at.day).zfill(2),
                                    str(created_at.hour).zfill(2))
            self._make_sure_path_exists(save_dir)
            tweet_file = os.path.join(save_dir, 'tweets.json')

            with open(tweet_file, 'a') as f:
                f.write(tweet)
                self._tweetCounter += 1
                # logger.info("Saved %d tweets." % self._tweetCounter)
                # sys.stdout.write("\rSaved %d tweets." % self._tweetCounter)
                # sys.stdout.flush()
                f.close()


        except Exception, e:
            logger.exception(e)
            return


class SaveTweetsListener(StreamListener):
    """ A listener that saves tweets to a specified directory
    """
    def __init__(self, tweet_saver=None, api=None):

        super(SaveTweetsListener, self).__init__(api=api)
        self._tweet_saver = tweet_saver

        if tweet_saver is None:
            raise Exception("Need a tweet saver!")

    def on_data(self, raw_data):
        """Run when data comes through. Write raw_data to file.
        """
        super(SaveTweetsListener, self).on_data(raw_data)
        self._tweet_saver.saveTweet(raw_data)
        tweetQueue.put(raw_data)

    def on_error(self, status):
        logger.warn(status)


class service(SocketServer.BaseRequestHandler):
    def handle(self):
        data = 'dummy'
        print "Client connected with ", self.client_address
        while True:
            data = tweetQueue.get()
            try:
                self.request.send(data)
            except:
                print "Client exited"
                self.request.close()
                break


class ThreadedTCPServer(SocketServer.ThreadingMixIn, SocketServer.TCPServer):
    pass

def run_tweet_server():
    print "Tweet Server started"
    server = ThreadedTCPServer(('0.0.0.0',1520), service)
    server.serve_forever()

def run_tweet_capture(query_string):
    query = [x.strip() for x in query_string.split(',')]
    print("Listening for tweets containing: %s" % ', '.join(query))
    stream = Stream(auth, l)
    stream.filter(track=query)

def parseOptions():
    parser = OptionParser()
    parser.add_option("-q", "--query", dest="query",
                      help="Quoted, comma-sepparated list of queries.",
                      metavar='"Phillies, Red Sox"')
    parser.add_option("-d", "--dir", dest="directory",
                      default=".", metavar="DIR",
                      help="Directory to save the tweets to.")
    parser.add_option("-I", dest="index_tweets", action="store_true",
                      help="Save tweets to an elasticsearch index")
    parser.add_option("-i", "--index", dest="index", default="default",
                      help="Index to save tweets to for elasticsearch.")
    parser.add_option("-t", "--type", dest="type", default="tweet",
                      help="Document type.")

    return parser.parse_args()

if __name__ == '__main__':
    try:
        (options, args) = parseOptions()
        tweet_saver = TweetSaver(save_dir="../data")

        if config.has_section('Proxy'):
            api = API(proxy=config.get('Proxy', 'https_proxy'))
        else:
            api = API()

        l = SaveTweetsListener(tweet_saver=tweet_saver, api=api)

        auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
        auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)

        if not options.query:
            print "Query required."
            sys.exit(1)

        query_string = options.query
        tweetQueue = Queue()

# Process variables
        global p1
        global p2

        print("Starting ....")

        p1 = Process(target=run_tweet_server)
        print("Starting Tweets ....")
        p1.start()

        p2 = Process(target=run_tweet_capture, args=(query_string,))
        print("Starting Capture ....")
        p2.start()

        rest_app.run(host='0.0.0.0', port=8088)

    except DirNotFoundException, e:
        logger.warn(e)
        sys.exit(1)

    except KeyboardInterrupt:
        logger.warn("Keyboard interrupt... exiting.")
        sys.exit(1)

#   except Exception:
#       raise

from tweepy import Stream, OAuthHandler
import json
from tweepy.streaming import StreamListener
from pymongo import MongoClient



API_KEY = "P4PQhDYwzAUBPNmEoGUcufBby"
API_SECRET_KEY = "WvKj4ZYIBDzS02h2GJ1jOK0opMkF5SxxToovIpkDgcVBoXgYsD"
ACCESS_TOKEN = "1131050145712398336-t4va4wnjFbP8TCzOnnOa6g2XW6Mq91"
ACCESS_TOKEN_SECRET = "RAb9h7XHu37bFXvmoGS9UBNdlTenAqSr2vqYfYLj6niWB"
MONGO_HOST= 'mongodb://localhost/twitterdb'



class StreamListener(StreamListener):
    # This is a class provided by tweepy to access the Twitter Streaming API.

    def __init__(self):
        super().__init__()
        self.max_tweets = 10000
        self.tweet_count = 0

    def on_connect(self):
        # Called initially to connect to the Streaming API
        print("You are now connected to the streaming API.")

    def on_error(self, status_code):
        # On error - if an error occurs, display the error / status code
        print('An Error has occured: ' + repr(status_code))
        return False

    def on_data(self, data):
        # This is the meat of the script...it connects to your mongoDB and stores the tweet
        try:
            client = MongoClient('localhost',27017)

            # Use twitterdb database. If it doesn't exist, it will be created.
            db = client.twitterdb

            # Decode the JSON from Twitter
            datajson = json.loads(data)

            # grab the 'created_at' data from the Tweet to use for display
            created_at = datajson['created_at']

            # print out a message to the screen that we have collected a tweet
            print("Tweet collected at " + str(created_at))

            # insert the data into the mongoDB into a collection called twitter_search
            # if twitter_search doesn't exist, it will be created.
            db.twitter_search.insert_one(datajson)
            self.tweet_count += 1
            if (self.tweet_count == self.max_tweets):
                print("completed")
                return (False)
        except Exception as e:
            print(e)



auth = OAuthHandler(API_KEY, API_SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
tweetStream = Stream(auth, StreamListener())
tweetStream.filter(track=["covid","covid19"],languages=['en'])


def retrive_data():
    try:
        client = MongoClient('localhost', 27017)
        db = client.twitterdb
        pipeline1 = [ { "$group" : { "_id" : "$user.location", "count": { "$sum": 1 }} } ]
        results1 = db.twitter_search.aggregate(pipeline1)
        pipeline2 = [{"$match":{"user.location" :"USA"}}]
        results2 = db.twitter_search.aggregate(pipeline2)
        pipeline3 = [{"$match":{"user.location" :"USA"}},{"$group": { "_id": "null", "count": { "$sum": 1 } } }]
        results3 = db.twitter_search.aggregate(pipeline3)
        print("Query 1 - $group result")
        for result in results1:
            print(result)
        print("Query 2 - $match result")
        for result in results2:
            print(result)
        print("Query 3 - Aggregation pipeline result")
        for result in results3:
            print(result)
    except Exception as e:
        print(e)


retrive_data()

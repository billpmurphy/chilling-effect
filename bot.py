import json
import os
import time

import sqlite3
import tweepy


def find_matching_tweets(queries, api):
    """
    Search for tweets that match any one of a set of query strings.

    Args:
        queries: an iterable of queries to search
        api: a tweepy.API object

    Returns:
        a set of tweets that match any one of the queries provided
    """
    return [result for q in queries for result in api.search(q)]


def get_original_tweet(tweet):
    """
    If a tweet is a retweet, return the original tweet. Otherwise, return the
    tweet.

    Args:
        tweet: the tweet to be inspected

    Returns:
        The original tweet
    """
    if hasattr(tweet, "retweeted_status"):
        return tweet.retweeted_status
    return tweet


def tweet_has_link(tweet):
    """
    Does a tweet contain a link?

    Args:
        tweet: the tweet to be inspected

    Returns:
        True if the tweet contains a link, and False otherwise
    """
    return bool(len(tweet.entities.get("urls", [])))


class User_DB(object):

    def __init__(self, db):
        """
        Create a new database to hold a list of twitter users, if it doesn't
        exist already.

        Args:
            db: the filename of the database
        """
        if os.path.exists(db):
            self.con = sqlite3.connect(db)
        else:
            self.con = sqlite3.connect(db)
            self.con.executescript("CREATE TABLE Users (Name TEXT);")
            self.con.commit()
        return

    def get_users(self):
        """
        Returns:
            A list of all users in the database
        """
        with self.con:
            cursor = self.con.cursor()
            cursor.execute("SELECT Name from Users;")
            users_retweeted = cursor.fetchall()
        return users_retweeted

    def add_user(self, name):
        """
        Add a user to the database.

        Args:
            name: The name of the user to add
        """
        with self.con:
            cursor = self.con.cursor()
            cursor.execute("INSERT INTO Users VALUES ('%s')" % name)
        return

    def has_user(self, name):
        """
        Does a user exist in the database?

        Args:
            name: The name of the twitter user

        Returns:
            True if a user is in the database, and False otherwise
        """
        with self.con:
            cursor = self.con.cursor()
            cursor.execute("SELECT * FROM Users WHERE Name='%s'" % name)
            return bool(len(cursor.fetchall()))
        return


def main():
    # set up tweepy stuff
    with open("twitter_conf.json", "rb") as f:
        conf = json.loads(f.read())
    auth = tweepy.OAuthHandler(conf["consumer_key"], conf["consumer_secret"])
    auth.set_access_token(conf["access_token"], conf["access_token_secret"])
    api = tweepy.API(auth)

    # set up database
    db = User_DB("users.db")

    # the queries to search for
    queries = ["NSA watchlist", "CIA watchlist"]

    while True:
        # search for tweets that match our query strings
        new_tweets = find_matching_tweets(queries, api)
        for tweet in new_tweets:
            t = get_original_tweet(tweet)
            username = t.user.screen_name
            if not tweet_has_link(t) and not db.has_user(username):
                db.add_user(username)
                t.retweet()
                time.sleep(5) # wait 5 seconds after retweeting
        time.sleep(60 * 5) # wait 5 minutes after searching
    return


if __name__ == '__main__':
    main()

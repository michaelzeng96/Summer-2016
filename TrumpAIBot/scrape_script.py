from twitter import *
import requests
import oauth2
import json
import schedule
import time
import os
from pymongo import MongoClient


#information for twitter REST API connection
ACCESS_TOKEN = '483632966-0Zs2bb4qSKSEeawUg0w9GvxXZJcP7ZQJSKqrKEFP'
ACCESS_SECRET = 'iZMdKWHXOT8Bt3JNvwaFMt8iNVgaCC53SsbEeb6J0FKj1'
CONSUMER_KEY = 'SWczKCuFphGCUI9NjUUs2sKwT'
CONSUMER_SECRET = 'cflgqndF163BqlVD2PTtOjnF8QSS91FoyoH0KIzGPD95UQPR2s'
consumer = oauth2.Consumer(key = CONSUMER_KEY, secret = CONSUMER_SECRET)
access_token = oauth2.Token(key = ACCESS_TOKEN, secret = ACCESS_SECRET)
client = oauth2.Client(consumer, access_token)


#get all tweets through the timeline via pagination of tweet ID
max_id = None
total = 0
with open('C:/Wingz/TrumpAIBot/tweets.txt','wb') as outfile:
    for i in range(500):     
        if max_id is None:
            resource_url = 'https://api.twitter.com/1.1/statuses/user_timeline.json?user_id=25073877'
        else:
            resource_url = "https://api.twitter.com/1.1/statuses/user_timeline.json?user_id=25073877&max_id="+str(max_id)
        response, data = client.request(resource_url)
        data = json.loads(data)
        for tweet in data:
            if tweet['retweeted']:
                continue
            else:
                    try:
                        text = tweet['text']
                        if len(text) < 140:
                            left_over = 140 - len(text)
                            for i in range(left_over):
                                text = text + ' '                   
                        text = text[0:140]
                        json.dump(text, outfile)
                        total += 1
                    except:
                        continue
        if not max_id:
            max_id = tweet['id'] - 1
        elif tweet['id'] < max_id:
            max_id = tweet['id'] - 1
        print 'scraped '+str(total)+' original (non-RT) trump tweets\n'          

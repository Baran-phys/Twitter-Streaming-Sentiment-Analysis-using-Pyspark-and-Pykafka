
#importing a pyspark package without which one gets this error: "'JavaPackage' object is not callable"
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--conf spark.ui.port=4041 --packages org.apache.kafka:kafka_2.11:0.10.0.0,org.apache.kafka:kafka-clients:0.10.0.0  pyspark-shell'


import pykafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener
from pyspark import SparkContext
sc = SparkContext("local[1]", "KafkaSendStream")
#import twitter_config"



#TWITTER API CONFIGURATIONS
consumer_key = "consumer_key"
consumer_secret = "consumer_secret"
access_token = "access_token"
access_secret = "access_secret"



#TWITTER API AUTH
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)

#Twitter Stream Listener
class KafkaPushListener(StreamListener):          
    def __init__(self):
        #localhost:9092 = Default Zookeeper Producer Host and Port Adresses
        self.client = pykafka.KafkaClient("localhost:9092")

        #Get Producer that has topic name is Twitter
        self.producer = self.client.topics[bytes("twitter")].get_producer()
  
    def on_data(self, data):
        #Producer produces data for consumer
        #Data comes from Twitter
        self.producer.produce(bytes(data))
        #print(data['text'])
        return True
                                                                                                                                           
    def on_error(self, status):
        print(status)
        return True

#Twitter Stream Config
twitter_stream = Stream(auth, KafkaPushListener())

#Produce Data that has Bitcoin hashtag (Tweets)
twitter_stream.filter(track=['#Bitcoin'])



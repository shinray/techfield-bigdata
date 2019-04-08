# # # # Twitter API Libraries # # # #
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# # # # KAFKA Libraries # # # #
from kafka import SimpleProducer, KafkaClient

# # # # Other Libraries # # # #
import config # twitter_credential.py

class KafkaProducer():
    """
    This will send the streaming message to the Kafka consumer
    """
    def __init__(self):
        # Kafka Configuration
        self.topic = 'capstone4' # Your topic name
        self.kafka = KafkaClient('localhost:9092') # Your server address 

    def producer(self, data):
        producer = SimpleProducer(self.kafka)
        return producer.send_messages(self.topic, data.encode('utf-8'))
    

class TwitterStreamer():
    """
    Class for streaming and processing live tweets.
    """
    def __init__(self):
        pass

    def streamTweets(self, hash_tag_list):
        # This handles Twitter authetification and the connection to Twitter Streaming API
        listener = Listener()
        auth = OAuthHandler(config.CONSUMER_KEY, config.CONSUMER_SECRET)
        auth.set_access_token(config.ACCESS_TOKEN, config.ACCESS_TOKEN_SECRET)
        stream = Stream(auth, listener)

        # This line filter Twitter Streams to capture data by the keywords: 
        stream.filter(track=hash_tag_list)

class Listener(StreamListener):
    """
    This is a basic listener that just prints received tweets to stdout.
    """
    def __init__(self):
        pass

    def on_data(self, data):
        kafka_producer = KafkaProducer()
        kafka_producer.producer(data)
        print(data)
        return True          

    def on_error(self, status):
        print(status)


if __name__ == '__main__':
 
    # Authenticate using config.py and connect to Twitter Streaming API.
    # hash_tag_list = ["Trump", "TRUMP", "trump", "Republican", "REPUBLICAN", "republican"]  # You can set the hash tag list, you are interested in
    hash_tag_list = ["undertale","deltarune","mother3","earthbound","kumatora","nintendo","savethemanuals","worksonmymachine","asoiaf"]

    twitterStreamer = TwitterStreamer()
    twitterStreamer.streamTweets(hash_tag_list)

    

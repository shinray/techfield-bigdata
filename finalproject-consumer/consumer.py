import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

BOOTSTRAP_SERVERS = "localhost:9092"
groupName = "kafkaConsumerGroup"
topicName = "CryptoCompare"
topics = topicName.split(",")

kafkaParams = {"bootstrap.servers": BOOTSTRAP_SERVERS, "group.id": groupName}

if __name__ == "__main__":
    sc = SparkContext(appName="CryptoCompare")
    ssc = StreamingContext(sc, 30)  # batch duration

    sc.setLogLevel("WARN")
    ssc.checkpoint("checkpoint")

    # Define Kafka consumer
    stream = KafkaUtils.createDirectStream(ssc, topics, kafkaParams)
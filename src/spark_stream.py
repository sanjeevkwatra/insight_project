#!/usr/bin/python
# Process  messages in  topic "topic" and store results in Cassandra keyspace# "keyspace". Batch duration comes in as batch_duration
# Cassandra interface not yet implemented

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
import argparse
import sys
import datetime

ZOOKEEPER_SERVERS= 'ec2-52-52-231-158.us-west-1.compute.amazonaws.com:2181'

def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help = 'Kafka topic to process')
    parser.add_argument("--keyspace", help = 'Cassandra keyspace to store into')
    parser.add_argument("--batch_duration", type=int, default=30, help = 'Cassandra keyspace to store into')
    args = parser.parse_args()
    return args


def parse_json_message(x):
   dict= json.loads(x[1])
   return (dict['series'],int(dict['timestamp']),int(dict['value']))


#main starts here
args = process_arguments()
sc = SparkContext(appName="insight_project")
ssc = StreamingContext(sc, args.batch_duration)

kvs = KafkaUtils.createStream(ssc, ZOOKEEPER_SERVERS, "group1", {args.topic:2})
lines = kvs.map(parse_json_message)
counts = lines.map(lambda x: (x[0],x[2]))\
         .reduceByKey(lambda a, b: a+b)
counts.pprint()

ssc.start()   
ssc.awaitTermination() 

#!/usr/bin/python
# Process  messages in  topic "topic" and store results in Cassandra keyspace# "keyspace". Batch duration comes in as batch_duration
# Cassandra interface not yet implemented

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster


import json
import argparse
import sys
import datetime

ZOOKEEPER_SERVERS= 'ec2-52-52-231-158.us-west-1.compute.amazonaws.com:2181'
CASSANDRA_SERVERS= ['ec2-13-56-105-222.us-west-1.compute.amazonaws.com', 'ec2-54-67-109-59.us-west-1.compute.amazonaws.com']


def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help = 'Kafka topic to process')
    parser.add_argument("--keyspace", help = 'Cassandra keyspace to store into')
    parser.add_argument("--batch_duration", type=int, default=30, help = 'Cassandra keyspace to store into')
    args = parser.parse_args()
    return args

def write_to_cassandra(rdd, session):
    msgs = rdd.collect()
    for msg in msgs:
       print msg
       dt = datetime.datetime.fromtimestamp(msg[1])
       session.execute("INSERT INTO series_data (series, year, month, day, hour, minute, timestamp, value) values (%s,%s,%s,%s,%s,%s,%s,%s)",(msg[0],dt.year,dt.month,dt.day, dt.hour, dt.minute, dt, msg[2]))

def parse_json_message(x):
   dict= json.loads(x[1])
   return (dict['series'],int(dict['timestamp']),int(dict['value']))


#main starts here
args = process_arguments()
sc = SparkContext(appName="insight_project")
ssc = StreamingContext(sc, args.batch_duration)

cassandra_cluster = Cluster(CASSANDRA_SERVERS)
cassandra_session = cassandra_cluster.connect(args.keyspace)

kafka_stream = KafkaUtils.createStream(ssc, ZOOKEEPER_SERVERS, "group1", {args.topic:2})
new_messages = kafka_stream.map(parse_json_message)
#counts = lines.map(lambda x: (x[0],x[2]))\
         #.reduceByKey(lambda a, b: a+b)
new_messages.foreachRDD(lambda rdd :write_to_cassandra(rdd,cassandra_session))

ssc.start()   
ssc.awaitTermination() 

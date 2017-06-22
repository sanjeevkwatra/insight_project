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

class PipelineContext:
    def __init__(self, topic, batch_duration, keyspace):
       self._ZOOKEEPER_SERVERS= 'ec2-52-52-231-158.us-west-1.compute.amazonaws.com:2181'
       self._CASSANDRA_SERVERS= ['ec2-13-56-105-222.us-west-1.compute.amazonaws.com', 'ec2-54-67-109-59.us-west-1.compute.amazonaws.com']
       self._sc = SparkContext(appName="insight_project")
       self._ssc = StreamingContext(self._sc, batch_duration)
       self._cluster = Cluster(self._CASSANDRA_SERVERS)
       self._session = self._cluster.connect(keyspace)
       self._kafka_stream = KafkaUtils.createStream(self._ssc, self._ZOOKEEPER_SERVERS, "group1", {topic:2})
    def kafka_stream(self):
        return self._kafka_stream
    def spark_context(self):
        return self._sc
    def cassandra_session(self):
        return self._session
    def start(self):
        self._ssc.start()
        self._ssc.awaitTermination()



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

def print_averages(x):
    averages= x.collect()
    print "\n\n\nAVERAGES\n\n\n"
    for avg in averages:
        print "%s: sum:%d count:%d avg:%f" % (avg[0], avg[1][0], avg[1][1], avg[1][0]/avg[1][1]) 


def updateFunction(newdata, olddata):
    if olddata is None:
        return (newdata[0], newdata[1])
    return (olddata[0]+ newdata[0],olddata[1]+ newdata[1])

def print_cumulative_rdd(rdd):
    print "Cumulative RDD start"
    for m in rdd.collect():
       print m
    print "Cumulative RDD end"

def sum_rdd(newdata):
    global cumulative_rdd
    if cumulative_rdd is None:
        cumulative_rdd = newdata.map(lambda x: x)
    else:
        cumulative_rdd= cumulative_rdd.union(newdata).reduceByKey(reducefunc)
        print_cumulative_rdd(cumulative_rdd)

def window_rdd(newdata):
    global cumulative_rdd
    if cumulative_rdd is None:
        cumulative_rdd = newdata.map(lambda x: x)
    else:
        cumulative_rdd= cumulative_rdd.leftOuterJoin(newdata)\
                        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1] + [x[1][1]])))\
                        .map(lambda x: (x[0],(x[1][0],x[1][1][-x[1][0]:])))
        print_cumulative_rdd(cumulative_rdd)

    
#main starts here
args = process_arguments()
pipeline = PipelineContext(args.topic, args.batch_duration, args.keyspace)
new_messages = pipeline.kafka_stream().map(parse_json_message)


"""
 The initial value of cumulative_rdd determines the windowing behavior of
 each series. For e.g. 'test' keeps a running window of size 3 micro-batches. 
 The array contains the 3 latest values of 'test', starting with the given
 initital values set to None. 
"""
cumulative_rdd = pipeline.spark_context()\
            .parallelize([('test',(3,[None,None,None])),\
                          ('blah',(2,[None,None]))])

new_messages.foreachRDD(lambda rdd :write_to_cassandra(rdd,pipeline.cassandra_session()))


mapped = new_messages.map(lambda x: (x[0],(int(x[2]),int(x[2]),int(x[2]), 1)))
reducefunc = lambda a, b:(max(a[0],b[0]),min(a[1],b[1]),a[2]+b[2],a[3] + b[3])
counts= mapped.reduceByKey(reducefunc)

counts.foreachRDD(window_rdd)

pipeline.start()


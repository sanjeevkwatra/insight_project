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

def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help = 'Kafka topic to process')
    parser.add_argument("--keyspace", help = 'Cassandra keyspace to store into')
    parser.add_argument("--batch_duration", type=int, default=30, help = 'Cassandra keyspace to store into')
    parser.add_argument('--window_sizes', type=argparse.FileType('r'), default=None,help="Name of file giving window sizes for seires in CSV format:<series>,<win size>")
    args = parser.parse_args()
    return args

def write_to_cassandra(rdd, session):
    msgs = rdd.collect()
    for msg in msgs:
       print msg
       dt = datetime.datetime.fromtimestamp(msg[1])
       session.execute("INSERT INTO series_data (series, year, month, day, hour, minute, timestamp, value) values (%s,%s,%s,%s,%s,%s,%s,%s)",(msg[0],dt.year,dt.month,dt.day, dt.hour, dt.minute, dt, msg[2]))


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
  
    """
Class WindowState manages the sliding windows for all series.
 The constructor for WindowState takes the  window sizes from a file 
 which comes in as a command line argument 
 _cumulative_rdd  will keep the "reduced state" foreach series -- last 2 
  micro-batches for series1, and last 4 for series8. The initial state
 is stored in an rdd as : 
  [('series2',(2,[None,None])), ('series2',(2,[None,None,None,None]))
  As micro-batches are pressed the "reduced state" comes in the arrays shift
  left, the rightmost element is the latest state, and the leftmost one is
  discarded. Only the state of series of series specified in window_dict
  are tracked across micro-batches, other series are only processed in
  micro-batches
  accumulate_rdd adds newdata (from the latest microbatch) on to 
  _cumulative_rdd. Data from the oldest batches (depending on the size
  of window for each series) is discarded
    """
class WindowState:
    def __init__(self,pipeline,window_sizes):
        windows_list=[] 
        for line in window_sizes:
            (series,window_size) = line.strip().split(',')
            window_size= int(window_size)
            windows_list.append((series,(window_size,[None] * window_size)))
        self._cumulative_rdd = pipeline.spark_context()\
            .parallelize(windows_list)
        
    def accumulate_rdd(self, dstream, newdata):
        #dstream is to be ignored -- an artifact of calling from foreachRDD
        if self._cumulative_rdd is not None:
            self._cumulative_rdd= self._cumulative_rdd.leftOuterJoin(newdata)\
                        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1] + [x[1][1]])))\
                        .map(lambda x: (x[0],(x[1][0],x[1][1][-x[1][0]:])))
        self.print_cumulative_rdd()

    def print_cumulative_rdd(self):
        print "Cumulative RDD start"
        for m in self._cumulative_rdd.collect():
            print m
        print "Cumulative RDD end"


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

pipeline = PipelineContext(args.topic, args.batch_duration, args.keyspace)

# initialiaze window state
windowstate = WindowState(pipeline, args.window_sizes)

# new_messages contains data from a new micro-batch
new_messages = pipeline.kafka_stream().map(parse_json_message)

#save message to cassandra with series as the partition key and year,month,
# date, hour, minute, timestamp as the clustering keys
new_messages.foreachRDD(lambda rdd :write_to_cassandra(rdd,pipeline.cassandra_session()))

# map produces tuples of type (series,(value,value,value,value))
mapped = new_messages.map(lambda x: (x[0],(int(x[2]),int(x[2]),int(x[2]), 1)))
#reduce produces tuples of type (series,(max, min, sum, count))
reducefunc = lambda a, b:(max(a[0],b[0]),min(a[1],b[1]),a[2]+b[2],a[3] + b[3])
counts= mapped.reduceByKey(reducefunc)

counts.foreachRDD(windowstate.accumulate_rdd)

pipeline.start()


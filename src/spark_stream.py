#!/usr/bin/python

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from cassandra.cluster import Cluster
import json
import argparse
import sys
import datetime
import alerts

def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--topic", required=True, help = 'Kafka topic to process')
    parser.add_argument("--keyspace", help = 'Cassandra keyspace to store into')
    parser.add_argument("--batch_duration", type=int, default=30, help = 'Cassandra keyspace to store into')
    parser.add_argument('--window_sizes', type=argparse.FileType('r'), default=None,help="Name of file giving window sizes for seires in CSV format:<series>,<win size>")
    args = parser.parse_args()
    return args


class PipelineContext:
    def __init__(self, topic, batch_duration):
       self._ZOOKEEPER_SERVERS= 'ec2-52-52-231-158.us-west-1.compute.amazonaws.com:2181'
       self._sc = SparkContext(appName="insight_project")
       self._ssc = StreamingContext(self._sc, batch_duration)
       self._kafka_stream = KafkaUtils.createStream(self._ssc, self._ZOOKEEPER_SERVERS, "group1", {topic:2})

    def kafka_stream(self):
        return self._kafka_stream

    def spark_context(self):
        return self._sc

    def start(self):
        self._ssc.start()
        self._ssc.awaitTermination()

class Cassandra_DB:
    def __init__(self, keyspace):
       self._CASSANDRA_SERVERS= ['ec2-13-56-105-222.us-west-1.compute.amazonaws.com', 'ec2-54-67-109-59.us-west-1.compute.amazonaws.com']
       self._cluster = Cluster(self._CASSANDRA_SERVERS)
       self._session = self._cluster.connect(keyspace)


    def write_to_cassandra(self, dummy, rdd):
        count= rdd.count()
        msgs = rdd.take(min(count,1))
        print "batch size:%d " % count
        if count > 0:
            print "first: ", msgs[0]
        # collect() for testing only
        #for msg in rdd.collect():
            #print msg
            #write_one_row(msg, self._session)
        new= rdd.map(lambda x: write_one_row(x,self._session))
        
def write_one_row(msg, session):
    dt = datetime.datetime.fromtimestamp(msg[1])
    session.execute("INSERT INTO series_data (series, year, month, day, hour, minute, timestamp, value) values (%s,%s,%s,%s,%s,%s,%s,%s)",(msg[0],dt.year,dt.month,dt.day, dt.hour, dt.minute, dt, msg[2]))
    return msg[0]

  
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
            splits= line.strip().split(',')
            if len(splits) == 3:
               (series,window_size,aggregate_func) = splits
            else: 
               (series,window_size) = splits
               aggregate_func= None
            window_size= int(window_size)
            windows_list.append((series,(window_size,aggregate_func, [None] * window_size)))
        self._cumulative_rdd = pipeline.spark_context()\
            .parallelize(windows_list)
        
    def accumulate_rdd(self, dstream, newdata):
        #dstream is to be ignored -- an artifact of calling from foreachRDD
        #for m in newdata.collect():  # testing only
             #print m
        if self._cumulative_rdd is not None:
            self._cumulative_rdd= self._cumulative_rdd.leftOuterJoin(newdata)\
                        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2] + [x[1][1]])))\
                        .map(lambda x: (x[0],(x[1][0],x[1][1],x[1][2][-x[1][0]:])))
        self.print_cumulative_rdd()
        results = self._cumulative_rdd.map(lambda x: create_results(x))
        self.print_results(results)
    # the two methods below only uised for testing
    def print_results(self,rdd):
        print "Results/Alerts"
        for m in rdd.collect():
            print m
        print "Cumulative RDD end"

    def print_cumulative_rdd(self):
        print "Cumulative RDD start"
        for m in  self._cumulative_rdd.collect():
            print m
        print "Cumulative RDD end"


def parse_json_message(x):
    dict= json.loads(x[1])
    return (dict['series'],float(dict['timestamp']),int(dict['value']))

def create_results(series_macrobatch):
    alert_function = alerts.alert_funcs[series_macrobatch[1][1]]
    batch_data = series_macrobatch[1][2]
    results = alert_function(batch_data)
    return (series_macrobatch[0], results)
  

    
#main starts here
args = process_arguments()

pipeline = PipelineContext(args.topic, args.batch_duration)
database = Cassandra_DB(args.keyspace)

# initialiaze window state
windowstate = WindowState(pipeline, args.window_sizes)

# new_messages contains data from a new micro-batch
new_messages = pipeline.kafka_stream().map(parse_json_message)

#save message to cassandra with series as the partition key and year,month,
# date, hour, minute, timestamp as the clustering keys
new_messages.foreachRDD(database.write_to_cassandra)

# map produces tuples of type (series,(value,value,value,value))
mapped = new_messages.map(lambda x: (x[0],(int(x[2]),int(x[2]),int(x[2]), 1)))
#reduce produces tuples of type (series,(max, min, sum, count))
reducefunc = lambda a, b:(max(a[0],b[0]),min(a[1],b[1]),a[2]+b[2],a[3] + b[3])
counts= mapped.reduceByKey(reducefunc)

counts.foreachRDD(windowstate.accumulate_rdd)

pipeline.start()


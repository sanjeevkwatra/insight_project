#!/usr/bin/python
# Code to produce kafka messages in batch mode. This script takes
# in a eries name and reads <series>.data file previously created 
# by series_generate.py, 
# Reads  the records a line at a time and produces kafa messages. 
# The series_name  used in the messages is <series> 

from kafka import KafkaProducer
import json
import argparse
import sys
import datetime
import random
import time
import pdb

BOOTSTRAP_SERVERS= 'ec2-52-52-231-158.us-west-1.compute.amazonaws.com:9092'

def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", required=True, help = 'Series name for messages')
    parser.add_argument("--topic", required=True, help = 'Kafka topic to send messages to')
    parser.add_argument("--number", type=int, required=True, help = 'number of messages to produce for each series')
    parser.add_argument("--num_series", type=int, default=1, help = 'number of series. If more than 1, a number is generated to series to generate name')
    

    args = parser.parse_args()
    return args


def produce_messages(series, topic, number, num_series):
    if num_series==1:
        series_names=[series]
    else:
       series_names=[None] * num_series
       for i in xrange(num_series):
           series_names[i]= "%s_%d" %(series,i)
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, \
         linger_ms=10, \
         value_serializer=lambda v: json.dumps(v).encode('utf-8')) 
    ts = time.time()
    print "start time = " + datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
    value = 5 # constant value for now
    for _ in xrange(number):
        for i in xrange(num_series):
            #print series_names[i]
            producer.send(topic, {'series': series_names[i],\
                                  'timestamp': ts,\
                                  'value': value})
            ts += .001
    print "end time = " + datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    producer.flush()

#  main starts here
args = process_arguments()
produce_messages(args.series,args.topic, args.number, args.num_series)


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

BOOTSTRAP_SERVERS= 'ec2-52-52-231-158.us-west-1.compute.amazonaws.com:9092'

def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--series", required=True, help = '<series>.data is read for messages to produce')
    parser.add_argument("--topic", required=True, help = 'Kafka topic to send messages to')
    args = parser.parse_args()
    try: 
        infile = open(args.series+'.data', 'r')
    except IOError:
        print 'Could not open ' + args.series +'.data' + '. Quitting.'
        sys.exit()
    return (args, infile) 


def produce_messages(series, infile, topic):
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, \
         batch_size=65536,\
         value_serializer=lambda v: json.dumps(v).encode('utf-8')) 
    for line in infile:
        (timestamp, value)=line.split()
        print series, timestamp, value
        producer.send(topic, {'series': series,\
                                  'timestamp': timestamp,\
                                  'value': value})
    producer.flush()

#  main starts here
(args, infile) = process_arguments()
produce_messages(args.series, infile, args.topic)


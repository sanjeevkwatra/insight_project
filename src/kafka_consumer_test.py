#!/usr/bin/python
# Code to test kafka messages sent in batch mode. This script takes
# in a "topic" name and an "offset" and spits out all messages in the topic\
# starting at "offset"  

from kafka import KafkaConsumer
import argparse
import sys

BOOTSTRAP_SERVERS= 'ec2-52-52-231-158.us-west-1.compute.amazonaws.com:9092'

def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("topic", help = 'Topic name')
    parser.add_argument("--beginning", action='store_true', help = 'Reset topic to begnning')
    args = parser.parse_args()
    return args


def consume_messages(args):
    if args.beginning:
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS,
                                 auto_offset_reset='earliest')
    else:
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP_SERVERS)
    consumer.subscribe(args.topic)
    for msg in consumer:
        print msg


#  main starts here
args = process_arguments()
consume_messages(args)




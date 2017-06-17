#!/usr/bin/python
# Code to generate synthetic data series for the project
# This script generates a time series given parameters described
# in command line help

import argparse
import sys
import datetime
import random
import pdb

def process_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("series_name", help = 'The output file is <series_name>.data')
    parser.add_argument("--start_time", type=int, required=True, help = 'Epoch time for first message')
    parser.add_argument("--end_time", type=int, required=True, help = 'Epoch time for last message')
    parser.add_argument("--min", type=int, required=True, help = 'lower bound for value; must be >=0')
    parser.add_argument("--max", type=int, required=True, help = 'upper bound for value; must be >=min')
    parser.add_argument("--rate", type=float, required=True, help = 'number of messages per second')
    parser.add_argument("--type", required=True, help = 'Type of Series. Trend goes from min to max. Period goes from min to max and back to min in time "period"', choices=('random', 'trend', 'periodic'))
    parser.add_argument("--period", type=float, help = 'time period in seconds for periodic series')
    args = parser.parse_args()
    if args.start_time < 946684800:
        print 'start_time must be > 946684800 (1/1/2000). Quitting.'
        sys.exit()
    if args.start_time > args.end_time:
        print 'start_time is greater than end_time. Quitting.'
        sys.exit()
    if args.min < 0:
        print 'min must be >= 0 Quitting.'
        sys.exit()
    if args.min > args.max:
        print 'min is > max. Quitting.'
        sys.exit()
    if args.rate <= 0:
        print '"rate" must be > 0. Quitting.'
        sys.exit()
    return args

def generate_periodic(args, timegap, message_count ):
    #pdb.set_trace()
    time_counter = args.start_time
    value = float(args.min)
    increment= float(args.max - args.min)*2/args.period
    print "increment %8.2f" % increment 
    for _ in range(message_count):
        print "%d, %d" % (time_counter, int(value))
        value+= increment
        if value >= args.max:
            if value > args.max:
                value-= increment
            increment =  -increment
        elif value <=  args.min:
            if value < args.min:
                value-= increment
            increment =  -increment
        time_counter += timegap


def generate_trend(args, timegap, message_count ):
    time_counter = args.start_time
    value = float(args.min)
    increment= float(args.max - args.min)/message_count
    for _ in range(message_count):
        print "%d, %d" % (time_counter, int(value))
        time_counter += timegap
        value+= increment

def generate_random(args, timegap, message_count ):
    time_counter = args.start_time
    series_min= args.min
    series_range= args.max - args.min
    for _ in range(message_count):
        print "%d, %d" % (time_counter, args.min + int(series_range*random.random()))
        time_counter += timegap

def generate_series(args):
    start_time = datetime.datetime.fromtimestamp(args.start_time)
    end_time = datetime.datetime.fromtimestamp(args.end_time)
    print "Start " + start_time.strftime("%-m/%-d/%Y  %H:%M:%S")
    print "End   " + end_time.strftime("%-m/%-d/%Y  %H:%M:%S")
    timegap = 1/args.rate
    message_count = int((args.end_time - args.start_time)/timegap)
    print "message_count %d" % message_count
    if args.type == 'random':
        generate_random(args, timegap, message_count)
    elif args.type == 'trend':
        generate_trend(args, timegap, message_count)
    elif args.type == 'periodic':
        if args.period is None:
            print '"period" must be specified for periodic series. Quitting.'
            sys.exit()
        generate_periodic(args, timegap, message_count)
    


#  main starts here
args = process_arguments()
generate_series(args)




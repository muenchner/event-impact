#!/usr/bin/python2.7
"""Author: Mahalia Miller
Date: August 10, 2011
Builds graph of D7 sensors
"""
from networkx import *
import sys
import matplotlib.pyplot as plt
import sqlite3
from operator import itemgetter
import string
from math import cos, sin, radians, acos, pi

def run_on_file(filename):
    """opens and reads file"""
    try:
        f = open(filename, "r")
        rows = f.read()
        f.close()
    except IOError:
        print "The file does not exist, exiting gracefully"
        rows=None
    yield rows
def find_ids(rows, minPM, maxPM, fwy, direction):
    """Tokenizes string and returns list of sensors that are within specified corridor.

    Args:
        rows: a long string with some \n that indicate sensors
        minPM: postmile as int
        maxPM: postmile as int
        fwy: for example, 5
        direction: for example, 'S'
    Returns:
        lines: list of ids in corridor with id, postmile, direction, coordinates (list), and highway as a list
                e.g. 715898, 117.28, 'S', ['33.880183', '-118.021787'], 5]
    """
    lines = []
    road_list=['ML']
#    road_list=['ML','HV', 'FR']
    for row in rows:
        strings=string.split(row,'\n')[1:]
        for stringI in strings:
            tokens=string.split(stringI,'\t')
            if len(tokens)>1:
                if float(tokens[7])<maxPM and float(tokens[7])>minPM and int(tokens[1])==fwy and tokens[2]==direction and tokens[11] in road_list:
                    try:
                        lines.append([int(tokens[0]), float(tokens[7]), tokens[2], [float(tokens[8]), float(tokens[9])], int(tokens[1])])
                    except ValueError:
                        print 'bad tokens: ', tokens
                        continue    
    return lines
def find_all_ids(rows):
    """Tokenizes string and returns list of sensors that are within specified corridor.

    Args:
        rows: a long string with some \n that indicate sensors
        minPM: postmile as int
        maxPM: postmile as int
        fwy: for example, 5
    Returns:
        lines: list of ids in corridor with id, postmile, direction, coordinates (list), and highway as a list
                e.g. 715898, 117.28, 'S', ['33.880183', '-118.021787'], 5]
    """
    lines = []
    road_list=['ML']
#    road_list=['ML','HV', 'FR']
    for row in rows:
        strings=string.split(row,'\n')[1:]
        for stringI in strings:
            tokens=string.split(stringI,'\t')
            if len(tokens)>1:
                if tokens[11] in road_list:
                    try:
                        lines.append([int(tokens[0]), float(tokens[7]), tokens[2], [float(tokens[8]), float(tokens[9])], int(tokens[1])])
                    except ValueError:
                        print 'bad tokens: ', tokens
                        continue
    print lines
    return lines

def parse_direction(t, dir):
    if t=='SB' or t=='S/B' or t=='S': 
        dir='S'
    elif t=='EB' or t=='E/B' or t=='E':
        dir='E'
    elif t=='NB' or t=='N/B' or t=='N':
        dir='N'
    elif t=='WB' or t=='W/B' or t=='W':
        dir='W'
    elif t=='':
        pass
    else:
        pass
    return dir
def handle_special_cases(substring):
    """identifies some manually-observed cases in wrong format and fixes them"""
    if substring==['', 'EB/NB', '2'] or substring==['EB/NB', '2', ''] or substring==['', 'EB/NB', '2', '#1'] or substring==['', 'EB/NB', '2', '#2']:
        dir='E'
        road=2
    elif substring== ['', 'WB/SB', '2'] or substring==['WB/SB', '2', '']:
        dir='W'
        road=2
    elif substring==['WB210', '']:
        dir='W'
        road=210
    elif substring==['S', '605/W', '10', '']:
        dir='W'
        road=10 
    elif substring==['', 'NB5', 'TRK', 'RTE']:
        dir='N'
        road=5
    elif substring==['', 'S605/E10']:
        dir='E'
        road=10
    else:
        dir=None
        road=0
    return dir, road
def distance(lat1, lon1, lat2, lon2):

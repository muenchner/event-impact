#!/usr/bin/python2.7
"""Author: Mahalia Miller
Date: July 23, 2011, updated August 23, 2011
Project: Given an incident, what is its impact?
This code goes through all the incidents and finds impact regions. For each impact region, 
the cumulative delay and the incident-related cumulative delay are computed.

A feature vector is appended after each incident.
<time_of_day(minutes), postmile_on_i5, description_code, num_lanes, occ_at_start, 
v_at_start, v*_at_start-v_at_start, cum_incident_delay, cum_total_delay, 
police_duration, tot_duration>

"""

import string, csv, time, math, gzip, os, glob, sys
import matplotlib.pyplot as plt
import numpy as np
import matplotlib.mlab as mlab
import scipy.signal
from numpy import * 
from scipy.cluster.vq import *
import pylab
import matplotlib as mpl
from mpl_toolkits.mplot3d import Axes3D
from operator import itemgetter
from datetime import datetime
from dateutil.relativedelta import relativedelta
import random
import sqlite3
import scipy.signal
from networkx import *

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

def process_rain(rows):
    """Does following post-processing:
        - Transforms date
        - Transforms time of day
    Args:
        rows: a long string with some \n that indicate new incidents

    Returns:
        lines:
    """
    lines = [] #rain_in_hour_starting_at_this_time
    rainy_days=[] #days with rain
    for row in rows:
        strings=string.split(row,'\n')[1:]
        for stringI in strings:
            tokens=string.split(stringI,'\t')
            if len(tokens)>1:
                date_raw=str(tokens[0]);
                ww=time.strptime(date_raw, "%Y%m%d")
                time_raw=str(tokens[1])
                if len(time_raw)==1:
                    hours=time_raw
                    mins=0
                elif len(time_raw)==3:
                    hours=time_raw[0:1]
                    if time_raw[1:3]=='00':
                        mins=0
                    else:
                        mins= time_raw[1:3]
                elif len(time_raw)==4:
                    hours=time_raw[0:2]
                    if time_raw[2:4]=='00':
                        mins=0
                    else:
                        mins= time_raw[2:4]    
                else:
                    print 'time in bad format'
                    break
                    time_raw='0'+time_raw
                try:
                    hours=int(hours)
                    mins=int(mins)
                except TypeError:
                    print tokens  
                date=time.strftime("%m/%d/%Y", ww)
#                day=time.strftime("%w", ww) #0=Sunday, 1=Monday...7=Saturday
#                minutes_since_midnight=int(int(hours)*60+int(mins))
                rain=float(tokens[3])
                rainy_days.append(date)
                lines.append([date, hours, rain])
    return lines, rainy_days
def get_init_rain(min, date, rain_list):
    hour=int(min/60.0)
    for rain_date, rain_hour, rain in rain_list:
        if rain_date==date and rain_hour==hour:
            return rain
    return 0  
def get_readings(chosen_sensor, route, time_of_day, chosen_date, cur):
    """returns the flow-weighted average speed across all lanes at one sensor
    Args:
        chosen_sensor: id
        time_of_day: in minutes since midnight
        chosen_date: in format %m/%d/%Y
    Returns:
        speed: speed as float at closest time to time_of_day at this specific sensor on this specific day
    """
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    try:
        sensor=int(chosen_sensor)
    except TypeError:
        return (None,None,None,None)
        print 'sensor is likely nonetype'
        print chosen_sensor
        print time_of_day
        print chosen_date    
    if route==5:
        try:
            cur.execute("select minutes, aveSpeed, aveFlow, length from sensorsi5manual where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
            readings=check_time(cur.fetchone(), sensor, route, time_of_day, chosen_date, cur)
            return readings[1:]
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return (None,None,None)
    elif route==605:
        try:
            cur.execute("select minutes, aveSpeed, aveFlow, length from sensorsi605manual where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
            readings=check_time(cur.fetchone(), sensor, route, time_of_day, chosen_date, cur)
            return readings[1:]
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return (None,None,None)
    else:
        print chosen_sensor, chosen_date, time_of_day
        return (None,None,None)
#        print 'Sensor not found in manually chosen tables. Now resorting to massive table but it will be slow'
#        try:
#            cur.execute("select aveSpeed, aveFlow, length from sensors where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
#            return cur.fetchone()
#        except TypeError:
#            print chosen_sensor, chosen_date, time_of_day
#            return (None,None,None)     
def check_time(readings, sensor, route, t_min, t_a_date, cur):
    ''' checks that the result from readings is for the correct time'''
    try:
        if int(readings[0]) != int(t_min): #if the speedy method failed, then get recordings by the slow method that is more sure
            readings=[t_min]
            readings.append(get_speed(sensor, t_min, t_a_date))
            readings.append(get_flow(sensor, t_min, t_a_date))
            readings.append(get_length(sensor, t_min, t_a_date))
            return readings
        else:
            return readings
    except TypeError:
        readings=[t_min]
        readings.append(get_speed(sensor, t_min, t_a_date))
        readings.append(get_flow(sensor, t_min, t_a_date))
        readings.append(get_length(sensor, t_min, t_a_date)) 
        return readings
def get_init_readings(chosen_sensor, route, time_of_day, chosen_date, cur):
    """returns the flow-weighted average speed and occupancy across all lanes at one sensor as well as the occupancy at one sensor
    Args:
        chosen_sensor: id
        route: road such as 5 or 605
        time_of_day: in minutes since midnight
        chosen_date: in format %m/%d/%Y
    Returns:
        (speed, occ): speed and occupancy as floats at closest time to time_of_day at this specific sensor on this specific day
    """
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    try:
        sensor=int(chosen_sensor)
    except TypeError:
        return (None,None)
        print 'sensor is likely nonetype'
        print chosen_sensor
        print time_of_day
        print chosen_date    
    if route==5:
        try:
            cur.execute("select minutes, aveSpeed, aveOcc from sensorsi5manual where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
            results=cur.fetchone()
            if int(results[0]) == int(time_of_day):
                return results[1:]
            else:
                results=[]
                results.append(get_occ(chosen_sensor, time_of_day, chosen_date, cur))
                results.append(get_speed(chosen_sensor, time_of_day, chosen_date, cur))
                return results
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return (None,None,None,None)
    elif route==605:
        try:
            cur.execute("select minutes, aveSpeed, occ from sensorsi605manual where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
            return cur.fetchone()
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return (None,None,None,None)
    else:
        print 'Sensor not found in manually chosen tables. Now resorting to massive table but it will be slow'
        try:
            cur.execute("select aveSpeed, aveFlow, length from sensors where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
            return cur.fetchone()
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return (None,None,None,None)     
def get_speed(chosen_sensor, time_of_day, chosen_date, cur):
    """returns the flow-weighted average speed across all lanes at one sensor
    Args:
        chosen_sensor: id
        time_of_day: in minutes since midnight
        chosen_date: in format %m/%d/%Y
    Returns:
        speed: speed as float at closest time to time_of_day at this specific sensor on this specific day
    """
    start=time.time()
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    try:
        sensor=int(chosen_sensor)
    except TypeError:
        return None
        print 'sensor is likely nonetype'
        print chosen_sensor
        print time_of_day
        print chosen_date    
    cur.execute("select aveSpeed from sensorsi5manual where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
    try:
        v= cur.fetchone()[0]
        print 'time to fetch plus prep', time.time()-start
        return v
    except TypeError:
        print 'except!'
        try: 
            cur.execute("select aveSpeed from sensorsi605manual where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
            return cur.fetchone()[0]
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return None
def get_vS(chosen_sensor, time_of_day,  cur):
    """returns the v* value, which is the threshold for congestion at a given location and time of day
    Args:
        chosen_sensor: id
        time_of_day: in minutes since midnight
    Returns:
        speed: speed as float at closest time to time_of_day at this specific sensor on this specific day
    """
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    try:
        cur.execute("select vStar from vStarValsMedianWeek where sensorID=? and minutes=?", (int(chosen_sensor), time_of_day))
        vs=cur.fetchone()[0]
        if vs==None:
            return 50
        else:
            return vs
    except TypeError:
        print 'No vstar value'
        return 50
    
        
def get_length(chosen_sensor, time_of_day, chosen_date, cur):
    """returns the flow-weighted average speed across all lanes at one sensor
    Args:
        chosen_sensor: id
        time_of_day: in minutes since midnight
        chosen_date: in format %m/%d/%Y
    Returns:
        speed: speed as float at closest time to time_of_day at this specific sensor on this specific day
    """
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    cur.execute("select length from sensorsi5manual where sensorID=? and date=? and minutes=?", (int(chosen_sensor), chosen_date, time_of_day))
    try:
        return cur.fetchone()[0]
    except TypeError:
        try: 
            cur.execute("select aveSpeed from sensorsi605manual where sensorID=? and date=? and minutes=?", (int(chosen_sensor), chosen_date, time_of_day))
            return cur.fetchone()[0]
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return None
def get_flow(chosen_sensor, time_of_day, chosen_date, cur):
    """returns the total flow across all lanes at one sensor
    Args:
        chosen_sensor: id
        time_of_day: in minutes since midnight
        chosen_date: in format %m/%d/%Y
    Returns:
        speed: speed as float at closest time to time_of_day at this specific sensor on this specific day
    """
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    cur.execute("select aveFlow from sensorsi5manual where sensorID=? and date=? and minutes=?", (int(chosen_sensor), chosen_date, time_of_day))
    try:
        return cur.fetchone()[0]
    except TypeError:
        try: 
            cur.execute("select aveFlow from sensorsi605manual where sensorID=? and date=? and minutes=?", (int(chosen_sensor), chosen_date, time_of_day))
            return cur.fetchone()[0]
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return None
def get_occ(chosen_sensor, time_of_day, chosen_date, cur):
    """returns the average occupancy (0 means empty highway, 1 means theoretical limit of bumper to buper) across all lanes at one sensor
    Args:
        chosen_sensor: id
        time_of_day: in minutes since midnight
        chosen_date: in format %m/%d/%Y
    Returns:
        occ: occupancy as float at closest time to time_of_day at this specific sensor on this specific day
    """
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    cur.execute("select aveOcc from sensorsi5manual where sensorID=? and date=? and minutes=?", (int(chosen_sensor), chosen_date, time_of_day))
    try:
        return cur.fetchone()[0]
    except TypeError:
        try: 
            cur.execute("select aveOcc from sensorsi605manual where sensorID=? and date=? and minutes=?", (int(chosen_sensor), chosen_date, time_of_day))
            return cur.fetchone()[0]
        except TypeError:
            print chosen_sensor, chosen_date, time_of_day
            return None
def update_start_time(sensor, route, time_of_day, date, day_of_week, cur):
    """updates start time to be within 15 minutes of reported start time to earliest time that is below v_ref
    Args:
        sensor: id
        time_of_day: in minutes since midnight
        date: in format %m/%d/%Y
        sensor_data: big dataset that has info at 5-minute intervals for this corridor
        v_ref: reference speed below which counts as delay
    Returns:
        time_of_day: revised start time of incident in minutes since midnight   
"""
    k=0.1
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    v=get_readings(sensor, route, time_of_day, date, cur)[0]
    vs=get_vS(sensor,time_of_day,  cur)
    if v>(1.0+k)*vs:
        if get_readings(sensor, route, time_of_day-5, date, cur)[0]<(1.0+k)*get_vS(sensor, time_of_day-5, cur):#UPDATE TO FALSE WHEN DONE TESTING
                time_of_day=time_of_day-5
        else:
            if get_readings(sensor, route, time_of_day-10, date, cur)[0]<(1.0+k)*get_vS(sensor, time_of_day-10, cur):#UPDATE TO FALSE WHEN DONE TESTING
                time_of_day=time_of_day-10
            else:
                if get_readings(sensor, route, time_of_day-15, date, cur)[0]<(1.0+k)*get_vS(sensor, time_of_day-15, cur): #UPDATE TO FALSE WHEN DONE TESTING
                    time_of_day=time_of_day-15
                else: 
                    print 'ERROR: speed at sensor closest to incident is not congested in a 15 minute window'
                    return None, None, None                        
#                    if get_readings(sensor, route, time_of_day+5, date, cur)[0]<(1.0+k)*get_vS(sensor, time_of_day+5, cur):#UPDATE TO FALSE WHEN DONE TESTING
#                        time_of_day=time_of_day+5
#                    else:
#                        if get_readings(sensor, route, time_of_day+10, date, cur)[0]<(1.0+k)*get_vS(sensor, time_of_day+10, cur):#UPDATE TO FALSE WHEN DONE TESTING
#                            time_of_day=time_of_day+10
#                        else:
#                            if get_readings(sensor, route, time_of_day+15, date, cur)[0]<(1.0+k)*get_vS(sensor, time_of_day+15, cur): #UPDATE TO FALSE WHEN DONE TESTING
#                                time_of_day=time_of_day+15
#                            else: 
#                                print 'ERROR: speed at sensor closest to incident is not congested in a 15 minute window'
#                                return None, None, None
    if time_of_day<0: #decrement day and revise time_of_day
        time_of_day=time_of_day+1440
        try:
            ww=time.strptime(date, "%m/%d/%Y")
            new=datetime(int(time.strftime("%Y", ww)),int(time.strftime("%m",ww)), int(time.strftime("%d", ww)))+relativedelta(days=+-1)
            date=time.strftime("%m/%d/%Y", new.timetuple())                        
            print 'Rolled back to new day ', date
        except:
            print 'failed to roll back ', date        
    return time_of_day, date, day_of_week
def find_closest_sensor(accident_pm, accident_direction, accident_route, accending_boolean, cur):
    """Finds the sensor ID that has the closest PM while being upstream to incident"""
    sensor_id=None
    eligibles=[]
    for row in cur.execute("select sensorID, abs_postmile from devices where road_type=='ML' and route=? and direction=?", (accident_route, accident_direction)):
        eligibles.append([int(row[0]), row[1]])
    for id, pm in sorted(eligibles, key=itemgetter(1)):
        if pm<=accident_pm:
            sensor_id=int(id)
        elif pm>accident_pm:
            if accending_boolean==True:
                return int(sensor_id)
            else:
                return int(id)
        else:
            print 'not finding closest sensor'
            print 'at postmile: ', accident_pm
            return None
def get_upstream_sensors(sensor_id, DG):
    """returns the id and pm of the next sensor upstream or ids and pms of next sensors upstream if at freeway-freeway connection. 
    """
    upstream_ones=None
    vals=[]
    try:
        upstream_ones=DG.predecessors(int(sensor_id)) #gets parent(s) of node in graph
        for sensor in upstream_ones:
            pm=DG.node[sensor]['mile']
            vals.append([sensor, pm])
    except:
        pass
    if len(vals)>0:
        return vals
    else:
        print 'No upstream sensor found'
        return [[None, None]]
def get_downstream_sensors(sensor_id, DG):
    """returns the id and pm of the next sensor downstream or ids and pms of next sensors downstream if at freeway-freeway connection. 
    """
    downstream_ones=None
    vals=[]
    try:
        downstream_ones=DG.successors(int(sensor_id)) #gets child (children) of node in graph
        for sensor in downstream_ones:
            pm=DG.node[sensor]['mile']
            vals.append([sensor, pm])
    except:
        pass
    if len(vals)>0:
        return vals
    else:
        print 'No downstream sensor found'
        return [[None, None]]
def f7(seq):
    '''returns unique elements of list only'''
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]

def traverse(sensor, G, t_min, t_a_date, cur, newset,  oldset, inc_delay_t, tot_delay_t,parent_flag, depth):
    '''recursively travels upstream from incident adding sensors part of impact region to a set
    Args:
        sensor: id
        G: directed graph
        t_min: time 
        t_a_date: date 
        cur: cursor to database with sensor recordings
        newset: set of nodes at this time step in impact region
        speedset: list of lists with sublist containing pm, v, vs, sensor_id, road_name
        parent_flag: 1 if the parent was within a window
    Returns:
        (n/a)    
    '''   
    depth=depth+1   
#    if depth>5:
#        newset.append(tot_delay_t)
#        newset.append(inc_delay_t)
#        return newset
    #do process
    readings=get_readings(sensor, G.node[sensor]['road'], t_min, t_a_date, cur)
    v=readings[0]
    vs=get_vS(sensor,t_min,  cur)
    k=0.1
    my_flag=False
    if v==None:
        newset.append(tot_delay_t)
        newset.append(inc_delay_t)
        return newset
    if v>=(1.0+k)*float(vs) and sensor not in oldset: #clearly out of range
        newset.append(tot_delay_t)
        newset.append(inc_delay_t)
        return newset
    if (v<(1.0+k)*float(vs)) and ((v>(1.0-k)*float(vs))): #in fuzzy range
        if parent_flag==True: #was in fuzzy range for parent too
            if sensor not in oldset: # 'in fuzzy range twice, so not allowing continued progression'
                newset.append(tot_delay_t)
                newset.append(inc_delay_t)
                return newset        
        else: #parent wasn't fuzzy so we allow this one but set the flag to True
            my_flag=True
            newset.append(sensor)
            l=readings[2] #0.3 for example
            f=readings[1] #450 for example
            if (l is not None) and (f is not None) and (v is not None) and (tot_delay_t is not None) and (inc_delay_t is not None):
                if v<60.0 and vs<60.0:
                    tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                    inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/vs,0)  
                if v<60.0 and vs>=60.0:
                    tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                    inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
            else:
                print 'Error. length, flow or velocity are None values. Nothing added to delay for sensor: ', sensor
#            speedset.append([G.node[sensor]['mile'], v, vs, sensor, G.node[sensor]['road']])
    if (v<=(1.0-k)*float(vs)) and (v>-1): #clearly congested
        newset.append(sensor)
        l=readings[2] #0.3 for example
        f=readings[1] #450 for example
        if (l is not None) and (f is not None) and (v is not None) and (tot_delay_t is not None) and (inc_delay_t is not None):
            if v<60.0 and vs<60.0:
                tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/vs,0)  
            if v<60.0 and vs>=60.0:
                tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
        else:
            print 'Error. length, flow or velocity are None values. Nothing added to delay for sensor: ', sensor
#        speedset.append([G.node[sensor]['mile'], v, vs, sensor, G.node[sensor]['road']])    
    parent_list=get_upstream_sensors(sensor, G)
    if parent_list[0][0] == None:
        print 'sensor '+str(sensor)+' had no upstream sensors!'
        newset.append(tot_delay_t)
        newset.append(inc_delay_t)
        return newset
    for parent in parent_list:
        if len(newset)>0:
            
            try:
                if float(newset[-1])<700000: #in this case, delay was appended already to the end of newset
#                    print 'pre-pop newset: ', newset
                    inc_delay_t=newset.pop()
                    tot_delay_t=newset.pop()
            except IndexError:
                print 'index error in traverse since newset is not a list' 
        traverse(parent[0], G, t_min, t_a_date, cur, newset, oldset, inc_delay_t, tot_delay_t, my_flag, depth)
def traverse_0(sensor, G, t_min, t_a_date, cur, newset,  inc_delay_t, tot_delay_t, parent_flag, depth):
    '''recursively travels upstream from incident adding sensors part of impact region to a set
    Args:
        sensor: id
        G: directed graph
        t_min: time 
        t_a_date: date 
        cur: cursor to database with sensor recordings
        newset: set of nodes at this time step in impact region
        speedset: list of lists with sublist containing pm, v, vs, sensor_id, road_name
        parent_flag: 1 if the parent was within a window
    Returns:
        (n/a)    
    '''
        
    depth=depth+1   
#    if depth>5:
#        newset.append(tot_delay_t)
#        newset.append(inc_delay_t)
#        return newset
    #do process
    readings=get_readings(sensor, G.node[sensor]['road'], t_min, t_a_date, cur) 
    v=readings[0]
    vs=get_vS(sensor,t_min,  cur)
    k=0.1
    my_flag=False
    if v==None:
        newset.append(tot_delay_t)
        newset.append(inc_delay_t)
        return newset
    if v>=(1.0+k)*float(vs): #clearly out of range
        newset.append(tot_delay_t)
        newset.append(inc_delay_t)
        #print newset
        return newset
    if (v<(1.0+k)*float(vs)) and ((v>(1.0-k)*float(vs))): #in fuzzy range
        if parent_flag==True: #was in fuzzy range for parent too
            newset.append(tot_delay_t)
            newset.append(inc_delay_t)
            #print newset
            return newset
        else:
            #print 'on probation'
            my_flag=True
            newset.append(sensor)            
            l=readings[2] #0.3 for example
            f=readings[1] #450 for example            
            if (l is not None) and (f is not None) and (v is not None) and (tot_delay_t is not None) and (inc_delay_t is not None):
                if v<60.0 and vs<60.0:
                    tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                    inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/vs,0)  
                if v<60.0 and vs>=60.0:
                    tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                    inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
            else:
                print 'Error. length, flow or velocity are None values. Nothing added to delay for sensor: ', sensor
    if (v<(1.0-k)*float(vs)) and (v>-1): 
        newset.append(sensor)
        l=readings[2] #0.3 for example
        f=readings[1] #450 for example
        if (l is not None) and (f is not None) and (v is not None) and (tot_delay_t is not None) and (inc_delay_t is not None):
            if v<60.0 and vs<60.0:
                tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/vs,0)  
            if v<60.0 and vs>=60.0:
                tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
        else:
            print 'Error. length, flow or velocity are None values. Nothing added to delay for sensor: ', sensor   
    parent_list=get_upstream_sensors(sensor, G)
    if parent_list[0][0] == None:
        print 'sensor '+str(sensor)+' had no upstream sensors!'
        newset.append(tot_delay_t)
        newset.append(inc_delay_t)
        return newset
    for parent in parent_list:
        if len(newset)>0:
            
            try:
                if float(newset[-1])<700000: #in this case, delay was appended already to the end of newset
#                    print 'pre-pop newset: ', newset
                    inc_delay_t=newset.pop()
                    tot_delay_t=newset.pop()
            except IndexError:
                print 'index error in traverse since newset is not a list' 
        traverse_0(parent[0], G, t_min, t_a_date, cur, newset, inc_delay_t, tot_delay_t, my_flag, depth)
def plot_status(nodes):
    '''saves a plot of the graph at this time with given nodes.
    If anything goes wrong, it just passes!
    Args: 
        nodes: list of nodes to be plotted
    Returns:
        None
    '''
    print 'nodes: ', nodes
    try:
        SG=nx.read_gpickle("testSimpleGraph.gpickle")
        H=nx.Graph()
        H.position={}
        for v in SG:
            if v in nodes:
                H.add_node(v)
                H.position[v]=SG.position[v]
        for (u,v) in SG.edges():
            if (u in nodes) and (v in nodes):
                H.add_edge(u,v)
        node_color=[float(H.degree(v)) for v in H]
#        node_size=[3*(float(H.degree(v))) for v in H]
        plt.figure()
        nx.draw(H,H.position,node_color=node_color, node_size=6, width=6,alpha=0.4,edge_color='0.75',font_size=0)
        nx.draw_networkx_labels(H,H.position,font_size=12,font_family='sans-serif')
#        plt.xlim(-118.302,-117.988)
#        plt.ylim(33.77,34.2)
        plt.xlim(-118.143,-118.059)
        plt.ylim(33.8908, 33.9781)
        plt.savefig(str(time.time())+'highwaymap.pdf')
#        plt.show()
        plt.close()

#    plt.show() #consider instead: http://networkx.lanl.gov/examples/graph/napoleon_russian_campaign.html
    except:
        pass
    pass
def plot_v_series(speedlist, t):
    '''saves a plot of the graph at this time with given nodes.
    If anything goes wrong, it just passes!
    Args: 
        speedlist: list with each element a list of pm, v, sensor, route
    Returns:
        None
    '''
    test_corridor=5
    try:
        x=[]
        y=[]
        ys=[]
        for pm, v, vstar, sensor, route in speedlist:
            if route==test_corridor and pm not in x:
                x.append(pm)
                y.append(v) 
                ys.append(vstar)       
        plt.figure()
        plt.plot(x,y, 'ko-', x,ys,'k--',linewidth=2)
        plt.plot(x,[60]*len(x),'r--')
        plt.ylabel('Speed [mph]')
        plt.xlabel('Postmile [miles]')
        plt.grid(True)
        plt.ylim(0,80)
        plt.savefig(str(t)+'_'+str(time.time())+'_vVSx.pdf')
#        plt.show()
        plt.close()
#    plt.show() #consider instead: http://networkx.lanl.gov/examples/graph/napoleon_russian_campaign.html
    except:
        pass
    pass
def main():
    G=nx.read_gpickle("test.gpickle")
    print 'number of nodes in graph: ', G.number_of_nodes()
    print 'number of edges in graph: ', len(G.edges())
    #determine when it was raining and by how much
    rows=run_on_file('wwp_accumulated_rain.txt')
    rain_list, rainy_day_list=process_rain(rows)
    db_file='d7Huge.db'
    tables=['vStarValsMedianWeek','sensorsi5manual', 'sensorsi605manual','devices', 'incidents']
    #load relevant tables into memory to improve performance
    conn = sqlite3.connect(':memory:')    
    cur = conn.cursor()
    cur.execute("attach database '" + db_file + "' as attached_db")
    for table_to_dump in tables:        
        cur.execute("select sql from attached_db.sqlite_master "
                   "where type='table' and name='" + table_to_dump + "'")
        sql_create_table = cur.fetchone()[0]
        cur.execute(sql_create_table);
        cur.execute("insert into " + table_to_dump +
                   " select * from attached_db." + table_to_dump)
        conn.commit()
    cur.execute("detach database attached_db")
    conn.commit()
    for row in cur.execute("select name from sqlite_master where type = 'table'"):
        print row   
#    con = sqlite3.connect('d7Huge.db')
#    cur = con.cursor()
    dates=[]
    for row in cur.execute("select date from incidents where route==5 and direction =='S' and abs_postmile>0 and abs_postmile<130 and duration>0 and day>0 and day<6 and holiday_boolean==0 "): #check this postmile condition   
        dates.append(str(row[0]))
    num_incidents=len(dates)
    u_dates=f7(dates)
#    u_dates=u_dates[33:]
    print u_dates
    print str(num_incidents)+' incidents here on '+str(len(set(dates)))+' days'
    for chosen_date in u_dates: #loop over days from d=1:k
        incident_list=[] #list of incidents on this day only
        start_tuples=[]
        for thing in cur.execute("select * from incidents where route==5 and direction=='S' and abs_postmile>116.9 and abs_postmile<130 and day>0 and day<6 and holiday_boolean==0 and date=?", (chosen_date,)): #check this postmile condition   
            incident_list.append(thing)
        for i in range(0,len(incident_list)):
            row=incident_list[i]
            accident_pm=row[10]
            accident_direction=row[9]
            accident_route=row[8]
            if (accident_direction=='N') or (accident_direction=='E'):
                accending_boolean=True
            else:
                accending_boolean=False
            u_a=find_closest_sensor(accident_pm, accident_direction, accident_route, accending_boolean, cur)                
            t_a_min=int(5.0*int(row[3]/5))
            incident_list[i]+=(t_a_min,) #start time
            incident_list[i]+=(u_a,) #closest upstream sensor
            start_tuples.append([t_a_min, u_a])
        print 'On '+str(chosen_date)+' there were '+str(len(incident_list))+' incidents'
        for row in incident_list:
            if len(row)>1:
                print 'incident details: ', row    
                accident_pm=row[10]
                accident_direction=row[9]
                accident_route=row[8]
                date=row[0]
                day=row[1]
                minutes=row[3]
                tot_delay=[]
                inc_delay=[]
                adjacent=0 #flag to say if impact region runs into impact region of another incident
                u_a=row[15]
#                print 'at sensor:', u_a
                cur.execute("select abs_postmile from devices where sensorID=?", (u_a, ))
                try:
                    u_a_pm=cur.fetchone()[0]
                except TypeError:
                    u_a_pm=None
                    print 'no pm for: ', accident_pm
                t_a_min=row[14] 
                t_a_date=date
                t_a_day=day
                t_a_min, t_a_date, t_a_day=update_start_time(u_a, accident_route, t_a_min, t_a_date, t_a_day, cur)
                relevant_date=t_a_date
                if t_a_min is not None:
#                    print 'at minutes:', minutes, t_a_min   
                    print 'At adjusted time of day '+str(t_a_min)+' of '+str(minutes)+' near sensor ID '+str(u_a)+' at pm '+str(u_a_pm)+' which should be upstream of accident pm '+str(accident_pm)                 
                    time_passing=True
                    t_min=t_a_min
                    newset=[]
                    parent_flag_0=False   
                    inc_delay_t=0
                    tot_delay_t=0
                    start=time.time()
                    traverse_0(u_a, G, t_a_min, relevant_date, cur, newset, inc_delay_t, tot_delay_t, parent_flag_0, 0)  
#                    print 'time to traverse 0', time.time()-start
                    try: 
                        inc_delay_t=newset.pop()
                        tot_delay_t=newset.pop()
                    except IndexError:
                        inc_delay_t=0
                        tot_delay_t=0
                    tot_delay.append(tot_delay_t)
                    inc_delay.append(inc_delay_t)
#                    print 'after time 0, set is : ',newset
                    if len(newset)<1:
                        time_passing=False
                    while time_passing:
                        on_probation=False
                        t_min=t_min+5 #starting time for this iteration
                        if t_min>1435: #hit midnight and need to rollover to new day!
                            t_min=0
                            try:
                                ww=time.strptime(relevant_date, "%m/%d/%Y")
                                new=datetime(int(time.strftime("%Y", ww)),int(time.strftime("%m",ww)), int(time.strftime("%d", ww)))+relativedelta(days=+1)
                                relevant_date=time.strftime("%m/%d/%Y", new.timetuple())                        
                                print 'Rolled over to new day ', relevant_date
                            except:
                                print 'failed to roll over ', relevant_date
                        if t_min-t_a_min>600:
                            print 'wayyy too long'
                            time_passing=False
                        for min, sens in start_tuples:
                            if min==t_min: 
                                on_probation=True
                        oldset=newset
                        newset=[]    
                        parent_flag=False
                        depth=0
                        inc_delay_t=0
                        tot_delay_t=0
                        start=time.time()
                        traverse(oldset[0], G, t_min, relevant_date, cur, newset, oldset, inc_delay_t, tot_delay_t, parent_flag, depth)         
                        try: 
                            inc_delay_t=newset.pop()
                            tot_delay_t=newset.pop()
                        except IndexError:
                            inc_delay_t=0
                            tot_delay_t=0
                        for sensor in newset:
                            if on_probation:
                                if [t_min, sensor] in start_tuples:
                                    print 'A new accident is starting here. So, the extent of this incident is terminating now.'
                                    time_passing=False
                                    adjacent=1
                                    break
                        tot_delay.append(tot_delay_t)
                        inc_delay.append(inc_delay_t)
                        if (t_min-t_a_min)%60==0: #save plot for every 60 minutes of recordings
                            print 'After an/another hour went by, time to traverse for one time step was: ', time.time()-start
                            print 'After an/another hour went by, for one time step newset has size: ', len(newset)

#                            print 'time: ', t_min
#                            print 'newset: ', newset
#                            plot_status(newset)
#                            plot_v_series(speedset,t_min)
                        if len(newset)<3:
                            time_passing=False
                else:
                    continue    
            #prepare data to append to text file with feature vector
            #get number of lanes at incident location
            cur.execute("select lanes from devices where sensorID=?", (u_a, ))
            try:
                lanes=cur.fetchone()[0]
            except TypeError:
                lanes='NA'
                print 'no lanes found for: ', u_a
            
            init_readings=get_init_readings(u_a, accident_route, t_a_min, t_a_date, cur)
            rho_0=init_readings[1]
            v_0=init_readings[0]
            vs_0=get_vS(u_a, t_a_min, cur)
            if rho_0 is None:
                rho_0='NA'
            if v_0 is None:
                v_0='NA'
                v_dif='NA'
            else:
                if vs_0==50:
                    v_dif='NA'
                else:
                    v_dif=vs_0-v_0
            init_readings5=get_init_readings(u_a, accident_route, t_a_min+5, t_a_date, cur)
            rho_05=init_readings5[1]
            v_05=init_readings5[0]
            vs_05=get_vS(u_a, t_a_min+5, cur)
            if rho_05 is None:
                rho_0='NA'
            if v_05 is None:
                v_05='NA'
                v_dif5='NA'
            else:
                if vs_05==50:
                    v_dif5='NA'
                else:
                    v_dif5=vs_05-v_05
            init_readings10=get_init_readings(u_a, accident_route, t_a_min+10, t_a_date, cur)
            rho_010=init_readings10[1]
            v_010=init_readings10[0]
            vs_010=get_vS(u_a, t_a_min+10, cur)
            if rho_010 is None:
                rho_010='NA'
            if v_0 is None:
                v_010='NA'
                v_dif10='NA'
            else:
                if vs_010==50:
                    v_dif10='NA'
                else:
                    v_dif10=vs_010-v_010
            u_a_up=get_upstream_sensors(u_a, G)[0][0]
            print 'up',  u_a_up
            init_readingsu=get_init_readings(u_a_up, accident_route, t_a_min, t_a_date, cur)
            rho_0u=init_readingsu[1]
            v_0u=init_readingsu[0]
            vs_0u=get_vS(u_a_up, t_a_min, cur)
            if rho_0u is None:
                rho_0u='NA'
            if v_0u is None:
                v_0u='NA'
                v_difu='NA'
            else:
                if vs_0u==50:
                    v_difu='NA'
                else:
                    v_difu=vs_0u-v_0u          
            init_readings5u=get_init_readings(u_a_up, accident_route, t_a_min+5, t_a_date, cur)
            rho_05u=init_readings5u[1]
            v_05u=init_readings5u[0]
            vs_05u=get_vS(u_a_up, t_a_min+5, cur)
            if rho_05u is None:
                rho_0u='NA'
            if v_05u is None:
                v_05u='NA'
                v_dif5u='NA'
            else:
                if vs_05u==50:
                    v_dif5u='NA'
                else:
                    v_dif5u=vs_05u-v_05u
            init_readings10u=get_init_readings(u_a_up, accident_route, t_a_min+10, t_a_date, cur)
            rho_010u=init_readings10u[1]
            v_010u=init_readings10u[0]
            vs_010u=get_vS(u_a_up, t_a_min+10, cur)
            if rho_010u is None:
                rho_010u='NA'
            if v_010u is None:
                v_010u='NA'
                v_dif10u='NA'
            else:
                if vs_010u==50:
                    v_dif10u='NA'
                else:
                    v_dif10u=vs_010u-v_010u     
            #determine how much it's raining
            try:
                rain=get_init_rain(t_a_min, t_a_date, rain_list) 
            except:
                rain='NA'   
            if t_a_date in rainy_day_list:
                rainy_day=1
            else:
                rainy_day=0   
            try:    
                feature_vector_line=str(minutes)+'\t'+str(accident_pm)+'\t'+str(row[13])+'\t'+str(rain)+'\t'+str(rainy_day)+'\t'+str(lanes)+'\t'+str(rho_0)+'\t'+str(v_0)+'\t'+str(v_dif)+'\t'+str(rho_05)+'\t'+str(v_05)+'\t'+str(v_dif5)+'\t'+str(rho_010)+'\t'+str(v_010)+'\t'+str(v_dif10)+'\t'+str(rho_0u)+'\t'+str(v_0u)+'\t'+str(v_difu)+'\t'+str(rho_05u)+'\t'+str(v_05u)+'\t'+str(v_dif5u)+'\t'+str(rho_010u)+'\t'+str(v_010u)+'\t'+str(v_dif10u)+'\t'+str(sum(inc_delay))+'\t'+str(sum(tot_delay))+'\t'+str(row[4])+'\t'+str(t_min-t_a_min)+'\t'+str(adjacent)+'\n' #TODO
            except:
                try:
                    feature_vector_line=str(minutes)+'\t'+str(accident_pm)+'\t'+str(row[13])+'\t'+str(rain)+'\t'+str(rainy_day)+'\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\t'+str(sum(inc_delay))+'\t'+str(sum(tot_delay))+'\t'+str(row[4])+'\t'+str(t_min-t_a_min)+'\t'+str(adjacent)+'\n'
                except:
                    feature_vector_line='NA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\n'    
            print feature_vector_line
            try:
                output_file=open('20110909_featureVector_nosize_min600_upstreamwRain.txt', "a")
                output_file.writelines(feature_vector_line)
                output_file.close()
            except:
                pass
      
if __name__ == '__main__':
    main()

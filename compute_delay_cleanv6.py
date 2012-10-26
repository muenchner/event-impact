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
import random
import sqlite3
import scipy.signal
from networkx import *


def get_speed(chosen_sensor, time_of_day, chosen_date, cur):
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
        return None
        print 'sensor is likely nonetype'
        print chosen_sensor
        print time_of_day
        print chosen_date
    cur.execute("select aveSpeed from sensorsi5manual where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
    try:
        return cur.fetchone()[0]
    except TypeError:
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
        cur.execute("select vStar from vStarValsMedian where sensorID=? and minutes=?", (int(chosen_sensor), time_of_day))
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
def update_start_time(sensor, time_of_day, date, day_of_week, cur):
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
    if time_of_day%5==0:
        time_of_day=int(time_of_day)
    else:
        time_of_day=int(5.0*int(time_of_day/5)) #round down
    if get_speed(sensor, time_of_day, date, cur)>get_vS(sensor, time_of_day, cur):
            if get_speed(sensor, time_of_day+5, date, cur)<get_vS(sensor, time_of_day, cur):#UPDATE TO FALSE WHEN DONE TESTING
                time_of_day=time_of_day+5
            else:
                if get_speed(sensor, time_of_day+10, date, cur)<get_vS(sensor, time_of_day, cur):#UPDATE TO FALSE WHEN DONE TESTING
                    time_of_day=time_of_day+10
                else:
                    if get_speed(sensor, time_of_day+15, date, cur)<get_vS(sensor, time_of_day, cur): #UPDATE TO FALSE WHEN DONE TESTING
                        time_of_day=time_of_day+15
                    else: 
                        print 'ERROR: speed at sensor closest to incident is not congested in a 15 minute window'
                        return None, None, None
    if time_of_day>1440:
        #TODO: increment day and revise time_of_day!!!!!!!!!!!!!!!!!!!!!!!
        pass
    return time_of_day, date, day_of_week
def find_closest_sensor(accident_pm, accident_direction, accident_route, accending_boolean, cur):
    """Finds the sensor ID that has the closest PM while being upstream to incident"""
    sensor_id=None
    eligibles=[]
    print 'accident direction: ', accident_direction
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
def f7(seq):
    '''returns unique elements of list only'''
    seen = set()
    seen_add = seen.add
    return [ x for x in seq if x not in seen and not seen_add(x)]


#def traverse(sensor, G, t_min, t_a_date, cur, newset, oldset, speedset):
#    '''recursively travels upstream from incident marking sensors as part of impact region
#    Args:
#        sensor: id
#        G: directed graph
#        t_min: time 
#        t_a_date: date 
#        cur: cursor to database with sensor recordings
#        newset: set of nodes at this time step in impact region
#        oldset: set of nodes at previous time step in impact region
#    Returns:
#    
#    
#    '''   
#    #do process
#    v=get_speed(sensor, t_min, t_a_date, cur)
#    vs=get_vS(sensor,t_min,  cur)
#    k=0.95
#    if v==None:
#        return newset
#    if v>=k*float(vs) and sensor not in oldset:
#        return newset
#    if (v<k*float(vs)) and (v>-1): 
#        print str(v)+' at sensor '+str(sensor)
#        print str(vs)+' is the v* value at sensor '+str(sensor)
#        newset.append(sensor)       
#        speedset.append([G.node[sensor]['mile'], v, vs, sensor, G.node[sensor]['road']])
#    parent_list=get_upstream_sensors(sensor, G)
#    if parent_list[0][0] == None:
#        print 'sensor '+str(sensor)+' had no upstream sensors!'
#        return newset
#    for parent in parent_list:
##        print 'parent '+str(parent)+' of sensor '+str(sensor)+' with parent(s) '+str(parent_list)
#        traverse(parent[0], G, t_min, t_a_date, cur,  newset, oldset, speedset)
##        print 'and now the set of nodes in impacted region is: ', newset
def traverse(sensor, G, t_min, t_a_date, cur, newset,  oldset, speedset,parent_flag):
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
    #do process
    v=get_speed(sensor, t_min, t_a_date, cur)
#    print str(v)+' at sensor '+str(sensor)
    vs=get_vS(sensor,t_min,  cur)
    k=0.05
    my_flag=False
    if v==None:
        return newset
    if v>=(1.0+k)*float(vs) and sensor not in oldset: #clearly out of range
        return newset
    if (v<(1.0+k)*float(vs)) and ((v>(1.0-k)*float(vs))): #in fuzzy range
        if parent_flag==True: #was in fuzzy range for parent too
            if sensor not in oldset:
                return newset
        else: #parent wasn't fuzzy so we allow this one but set the flag to True
            my_flag=True
            newset.append(sensor)
            speedset.append([G.node[sensor]['mile'], v, vs, sensor, G.node[sensor]['road']])
    if (v<(1.0-k)*float(vs)) and (v>-1): #clearly congested
        newset.append(sensor)
        speedset.append([G.node[sensor]['mile'], v, vs, sensor, G.node[sensor]['road']])    
    parent_list=get_upstream_sensors(sensor, G)
    if parent_list[0][0] == None:
        print 'sensor '+str(sensor)+' had no upstream sensors!'
        return newset
    for parent in parent_list:
#        print 'parent '+str(parent)+' of sensor '+str(sensor)+' with parent(s) '+str(parent_list)
        traverse(parent[0], G, t_min, t_a_date, cur, newset, oldset, speedset, my_flag)
        
#        print 'and now the set of nodes in impacted region is: ', newset
def traverse_0(sensor, G, t_min, t_a_date, cur, newset,  speedset,parent_flag):
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
    #do process
    v=get_speed(sensor, t_min, t_a_date, cur)
    print str(v)+' at sensor '+str(sensor)
    vs=get_vS(sensor,t_min,  cur)
    k=0.05
    my_flag=False
    if v==None:
        return newset
    if v>=(1.0+k)*float(vs): #clearly out of range
        return newset
    if (v<(1.0+k)*float(vs)) and ((v>(1.0-k)*float(vs))): #in fuzzy range
        if parent_flag==True: #was in fuzzy range for parent too
            return newset
        else:
            my_flag=True
            newset.append(sensor)
            speedset.append([G.node[sensor]['mile'], v, vs, sensor, G.node[sensor]['road']])
    if (v<(1.0-k)*float(vs)) and (v>-1): 
        newset.append(sensor)
        speedset.append([G.node[sensor]['mile'], v, vs, sensor, G.node[sensor]['road']])    
    parent_list=get_upstream_sensors(sensor, G)
    if parent_list[0][0] == None:
        print 'sensor '+str(sensor)+' had no upstream sensors!'
        return newset
    for parent in parent_list:
#        print 'parent '+str(parent)+' of sensor '+str(sensor)+' with parent(s) '+str(parent_list)
        traverse_0(parent[0], G, t_min, t_a_date, cur, newset, speedset, my_flag)
#        print 'and now the set of nodes in impacted region is: ', newset
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
    db_file='d7Huge3.db'
    tables=['vStarValsMedian','sensorsi5manual', 'sensorsi605manual','devices', 'incidents']
#    tables=['devices']
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
    for row in cur.execute("select date from incidents where route==5 and direction =='S' and abs_postmile>0 and abs_postmile<130 and day>0 and day<6 and holiday_boolean==0 and minutes>480 and minutes<1200"): #check this postmile condition   
        dates.append(str(row[0]))
    dates=dates[30:]
    print str(len(dates))+' incidents here on '+str(len(set(dates)))+' days'
    for chosen_date in set(dates): #loop over days from d=1:k
        incident_list=[] #list of incidents on this day only
        start_tuples=[]
        for thing in cur.execute("select * from incidents where route==5 and direction=='S' and abs_postmile>116.9 and abs_postmile<130 and day>0 and day<6 and holiday_boolean==0 and date=? and minutes>480 and minutes<1200", (chosen_date,)): #check this postmile condition   
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
            print row
            start_tuples.append([t_a_min, u_a])
        print start_tuples   
        print incident_list 
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
#                if (accident_direction=='N') or (accident_direction=='E'):
#                    accending_boolean=True
#                else:
#                    accending_boolean=False
#                u_a=find_closest_sensor(accident_pm, accident_direction, accident_route, accending_boolean, cur)
                u_a=row[15]
                print 'at sensor:', u_a
                cur.execute("select abs_postmile from devices where sensorID=?", (u_a, ))
                try:
                    u_a_pm=cur.fetchone()[0]
                    print 'at postmile: ', accident_pm, u_a_pm
                except TypeError:
                    u_a_pm=None
                    print 'no pm for: ', accident_pm
                t_a_min=row[14] 
                t_a_date=date
                t_a_day=day
                t_a_min, t_a_date, t_a_day=update_start_time(u_a, t_a_min, t_a_date, t_a_day, cur)
                if t_a_min is not None:
                    print 'at minutes:', minutes, t_a_min                    
                    time_passing=True
                    t_min=t_a_min
                    newset=[]
                    speedset=[]
                    parent_flag_0=False   
                    traverse_0(u_a, G, t_a_min, t_a_date, cur, newset, speedset, parent_flag_0)         
                    print 'after time 0, set is : ',newset
                    newset=f7(newset)
                    plot_status(newset)
                    plot_v_series(speedset,t_min)
                    if len(newset)<1:
                        time_passing=False
                    while time_passing:
                        on_probation=False
                        t_min=t_min+5 #starting time for this iteration
                        for min, sens in start_tuples:
                            if min==t_min: 
                                on_probation=True
                        oldset=newset
                        newset=[]    
                        speedset=[] 
                        starters=dict.fromkeys(oldset,0)  
                        for node in oldset:
    #                                print 'passing by node ', node
                            if starters[node]==0:
    #                                print 'as starting node, using node ', node
                                parent_flag=False
                                traverse(node, G, t_min, t_a_date, cur, newset, oldset, speedset, parent_flag)         
                                updaters=list(set(newset) & set(oldset))
                                starters.update(dict.fromkeys(updaters,1))
    #                        print 'newset was size: ', len(newset)
                        newset=f7(newset) 
                        print 'and now newset has size: ', len(newset)
                        tot_delay_t=0
                        inc_delay_t=0
                        for sensor in newset:
                            if on_probation:
                                if [t_min, sensor] in start_tuples:
                                    print 'A new accident is starting here. So, the extent of this incident is terminating now.'
                                    time_passing=False
                                    adjacent=1
                                    break
                            l=get_length(sensor, t_min, t_a_date, cur)
                            v=get_speed(sensor, t_min, t_a_date, cur)
                            vs=get_vS(sensor, t_min,  cur)
                            f=get_flow(sensor, t_min, t_a_date, cur)
                            if (l is not None) and (f is not None) and (v is not None) and (tot_delay_t is not None) and (inc_delay_t is not None):
                                if v<60.0 and vs<60.0:
                                    tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                                    inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/vs,0)  
                                if v<60.0 and vs>=60.0:
                                    tot_delay_t=tot_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                                    inc_delay_t=inc_delay_t+(5.0/60.0)*l*12.0*f*max(1.0/v-1.0/60.0,0)
                            else:
                                print 'Error. length, flow or velocity are None values. Nothing added to delay for sensor: ', sensor
                        tot_delay.append(tot_delay_t)
                        inc_delay.append(inc_delay_t)
                        if t_min%60==0: #save plot for every 60 minutes of recordings
                            print 'time: ', t_min
                            print 'newset: ', newset
                            plot_status(newset)
                            plot_v_series(speedset,t_min)
                        if len(newset)<3:
                            time_passing=False
                else:
                    continue    
            
            print tot_delay
            print inc_delay 
            #prepare data to append to text file with feature vector
            #get number of lanes at incident location
            cur.execute("select lanes from devices where sensorID=?", (u_a, ))
            try:
                lanes=cur.fetchone()[0]
            except TypeError:
                lanes='NA'
                print 'no lanes found for: ', u_a
            rho_0=get_occ(u_a, t_a_min, t_a_date, cur)
            v_0=get_speed(u_a, t_a_min, t_a_date, cur)
            vs_0=get_vS(u_a, t_a_min, cur)
            if rho_0 is None:
                rho_0='NA'
            if v_0 is None:
                v_0='NA'
                v_dif='NA'
            if vs_0==50:
                v_dif='NA'
            v_dif=vs_0-v_0
            try:    
                feature_vector_line=str(minutes)+'\t'+str(accident_pm)+'\t'+str(row[13])+'\t'+str(lanes)+'\t'+str(rho_0)+'\t'+str(v_0)+'\t'+str(v_dif)+'\t'+str(sum(inc_delay))+'\t'+str(sum(tot_delay))+'\t'+str(row[4])+'\t'+str(t_min-minutes)+'\t'+str(adjacent)+'\n' #TODO
            except:
                try:
                    feature_vector_line=str(minutes)+'\t'+str(accident_pm)+'\t'+str(row[13])+'\tNA\tNA\tNA\tNA\'+\t'+str(sum(inc_delay))+'\t'+str(sum(tot_delay))+'\t'+str(row[4])+'\t'+str(t_min-minutes)+'\t'+str(adjacent)+'\n'
                except:
                    feature_vector_line='NA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\tNA\n'    
            print feature_vector_line
            try:
                output_file=open('20110828_featureVector.txt', "a")
                output_file.writelines(feature_vector_line)
                output_file.close()
            except:
                pass
    #        try:
    #            text_file = open('20110823_tot'+str(accident_pm)+'AT'+str(minutes)+'ON'+t_a_date+'.txt', "w")
    #            text_file.writelines(tot_delay)
    #            text_file.close() 
    #            text_file = open('20110823_inc'+str(accident_pm)+'AT'+str(minutes)+t_a_date+'.txt', "w")
    #            text_file.writelines(inc_delay)
    #            text_file.close()   
    #            print 'Wrote delay totals for this incident'
    #        except:
    #            pass            
if __name__ == '__main__':
    main()
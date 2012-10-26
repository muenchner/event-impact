#!/usr/bin/python2.7
"""Author: Mahalia Miller
Date: August 10, 2011
Builds graph of D7 sensors
"""
import sqlite3
import string
from math import cos, sin, radians, acos, pi
import numpy as np

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
def find_ids(rows, fwy):
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
    MAX_PM=150 #TODO CHANGE THIS
    lines = []
    for row in rows:
        strings=string.split(row,'\n')[1:]
        for stringI in strings:
            tokens=string.split(stringI,'\t')
            if len(tokens)>1:
                if int(tokens[1])==fwy and tokens[11]=='ML' and float(tokens[7])<MAX_PM:
#                if int(tokens[1])==fwy and tokens[11]=='ML':
                    try:
                        lines.append(int(tokens[0]))
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
#    print lines
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
    """ finds the distance in miles between points"""
    earth_radius=3959.0 #miles
    if lat1==lat2 and lon1==lon2:
        dst=0
    else:
        dst = acos(
                    (sin(radians(lat1)) * sin(radians(lat2))) +
                    (cos(radians(lat1)) * cos(radians(lat2)) * cos(radians(lon1) - radians(lon2)))
            ) * earth_radius
    return dst

def nearest(coordinate, coordinate_list, limit=None):
    """finds nearest k points using great circle distance
    Args:
        coordinate: 2 element list with lat and lon only of reference point
        coordinate_list: candidate list of either coordinates or full line of a connector sensor with the 3rd item in the list the coordinates
        limit: number of points to return
    Returns:
        distances: tuple of distance and the corresponding element in coordinate_list of k closest points
    """
    distances = []
    coordinate_lat=coordinate[0]
    coordinate_lon=coordinate[1]
    for c in coordinate_list:
        if len(c)==5:
            distances.append( (distance(coordinate_lat, coordinate_lon, c[3][0], c[3][1]), c))
        else:
            distances.append( (distance(coordinate_lat, coordinate_lon, c[0], c[1]), c))            
    distances.sort()
    if limit:
        return distances[:limit]
    return distances

def toRadiant(self, distance):
    earth_radius=3959.0 #miles
    return distance / (earth_radius * 2 * pi) * 360 
#        
#def get_readings(chosen_sensor, route, time_of_day, chosen_date, cur):
#    """returns the flow-weighted average speed across all lanes at one sensor
#    Args:
#        chosen_sensor: id
#        time_of_day: in minutes since midnight
#        chosen_date: in format %m/%d/%Y
#    Returns:
#        speed: speed as float at closest time to time_of_day at this specific sensor on this specific day
#    """
#    if time_of_day%5==0:
#        time_of_day=int(time_of_day)
#    else:
#        time_of_day=int(5.0*int(time_of_day/5)) #round down
#    try:
#        sensor=int(chosen_sensor)
#    except TypeError:
#        return (None,None,None,None)
#        print 'sensor is likely nonetype'
#        print chosen_sensor
#        print time_of_day
#        print chosen_date    
#    if route==5:
#        try:
#            cur.execute("select minutes, aveSpeed from sensorsi5manual where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
#            readings=check_time(cur.fetchone(), sensor, route, time_of_day, chosen_date, cur)
#            return readings[1:]
#        except TypeError:
#            print chosen_sensor, chosen_date, time_of_day
#            return None
#    elif route==605:
#        if time_of_day<500:
#            try:
#                cur.execute("select minutes, aveSpeed from sensors where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
#                readings=check_time(cur.fetchone(), sensor, route, time_of_day, chosen_date, cur)
#                return readings[1:]
#            except:
#                return None 
#        else:
#            try:
#                cur.execute("select minutes, aveSpeed from sensorsi605manual where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
#                readings=check_time(cur.fetchone(), sensor, route, time_of_day, chosen_date, cur)
#                return readings[1:]
#            except:
#                cur.execute("select minutes, aveSpeed from sensors where sensorID=? and date=? and minutes>(?-1) and minutes<(?+16)", (sensor, chosen_date, time_of_day, time_of_day))
#                readings=check_time(cur.fetchone(), sensor, route, time_of_day, chosen_date, cur)
#                return readings[1:]
#    else:
#        print chosen_sensor, chosen_date, time_of_day
#        return None
##        print 'Sensor not found in manually chosen tables. Now resorting to massive table but it will be slow'
##        try:
##            cur.execute("select aveSpeed, aveFlow, length from sensors where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
##            return cur.fetchone()
##        except TypeError:
##            print chosen_sensor, chosen_date, time_of_day
##            return None     
#def check_time(readings, sensor, route, t_min, t_a_date, cur):
#    ''' checks that the result from readings is for the correct time'''
#    try:
#        if int(readings[0]) != int(t_min): #if the speedy method failed, then get recordings by the slow method that is more sure
#            readings=[t_min]
#            readings.append(get_speed(sensor, t_min, t_a_date))
#
#            return readings
#        else:
#            return readings
#    except TypeError:
#        readings=[t_min]
#        readings.append(get_speed(sensor, t_min, t_a_date))
#        return readings
#
#
#def get_speed(chosen_sensor, time_of_day, chosen_date, cur):
#    """returns the flow-weighted average speed across all lanes at one sensor
#    Args:
#        chosen_sensor: id
#        time_of_day: in minutes since midnight
#        chosen_date: in format %m/%d/%Y
#    Returns:
#        speed: speed as float at closest time to time_of_day at this specific sensor on this specific day
#    """
#    if time_of_day%5==0:
#        time_of_day=int(time_of_day)
#    else:
#        time_of_day=int(5.0*int(time_of_day/5)) #round down
#    try:
#        sensor=int(chosen_sensor)
#    except TypeError:
#        return None
#        print 'sensor is likely nonetype'
#        print chosen_sensor
#        print time_of_day
#        print chosen_date    
#    cur.execute("select aveSpeed from sensorsi5manual where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
#    try:
#        v= cur.fetchone()[0]        
#        return v
#    except TypeError:
#        print 'except!'
#        try: 
#            cur.execute("select aveSpeed from sensorsi605manual where sensorID=? and date=? and minutes=?", (sensor, chosen_date, time_of_day))
#            return cur.fetchone()[0]
#        except TypeError:
#            print chosen_sensor, chosen_date, time_of_day
#            return None           

def main():
#    db_file='d7Huge.db'
#    tables=['vStarValsMedian','sensorsi605manual','sensorsi605forvs']
##    tables=['devices']
#    #load relevant tables into memory to improve performance
#    con = sqlite3.connect(':memory:')    
#    cur = con.cursor()
#    cur.execute("attach database '" + db_file + "' as attached_db")
#    for table_to_dump in tables:        
#        cur.execute("select sql from attached_db.sqlite_master "
#                   "where type='table' and name='" + table_to_dump + "'")
#        sql_create_table = cur.fetchone()[0]
#        cur.execute(sql_create_table);
#        cur.execute("insert into " + table_to_dump +
#                   " select * from attached_db." + table_to_dump)
#        con.commit()
#    cur.execute("detach database attached_db")
#    con.commit()
#    for row in cur.execute("select name from sqlite_master where type = 'table'"):
#        print row   
       
    con=sqlite3.connect('d7Huge.db')
    cur = con.cursor()
    cur.execute('drop table vStarValsMedianWeek')
    cur.execute('''create table vStarValsMedianWeek (minutes real, sensorID real, vStar real)''')

    sensor_rows = run_on_file('d07_stations_2008_11_26.txt')
    subset_road_set=set([605])
    sensors=set()
    for road in subset_road_set:
        sensors|=set(find_ids(sensor_rows, road))        
    print 'finding medians for sensors numbering ', len(sensors)
    for sensorr in sensors:
        print sensorr
        for time in range(0,1436,5): #before, did time in range(480,600,5)
            SENSOR_LIST_0=[]
            SENSOR_LIST_5=[]
            SENSOR_LIST_10=[]
            SENSOR_LIST_15=[]
            try:
                cur.execute("select minutes, aveSpeed from sensorsi605manual where sensorID=? and minutes>(?-1) and minutes<(?+16) and day>0 and day<6 and holidayBoolean==0", (sensorr, time, time)) #check this postmile condition   
                rows=cur.fetchall()
                for index in range(0,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_0.append(rows[index][1])
                for index in range(1,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_5.append(rows[index][1])
                for index in range(2,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_10.append(rows[index][1])
                for index in range(3,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_15.append(rows[index][1])
                v_star_0=np.median(np.array(SENSOR_LIST_0))
                v_star_5=np.median(np.array(SENSOR_LIST_5))
                v_star_10=np.median(np.array(SENSOR_LIST_10))
                v_star_15=np.median(np.array(SENSOR_LIST_15))
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time, sensorr, v_star_0])
#                print 'inserted ', v_star_0
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time+5, sensorr, v_star_5])
#                print 'inserted ', v_star_5
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time+10, sensorr, v_star_10])
#                print 'inserted ', v_star_10
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time+15, sensorr, v_star_15])
                print 'inserted ', v_star_15
            except:
                print 'could not find this i605 value at minute: ', time
                pass#    for sensorr in sensors:
#        
        con.commit() 
    sensor_rows = run_on_file('d07_stations_2008_11_26.txt')
    subset_road_set=set([5])
    sensors=set()
    for road in subset_road_set:
        sensors|=set(find_ids(sensor_rows, road)) 
    print 'finding medians for sensors numbering ', len(sensors)
    for sensorr in sensors:
        print sensorr
        for time in range(0,1436,5): #before, did time in range(480,600,5)
            SENSOR_LIST_0=[]
            SENSOR_LIST_5=[]
            SENSOR_LIST_10=[]
            SENSOR_LIST_15=[]
            try:
                cur.execute("select minutes, aveSpeed from sensorsi5manual where sensorID=? and minutes>(?-1) and minutes<(?+16) and day>0 and day<6 and holidayBoolean==0", (sensorr, time, time)) #check this postmile condition   
                rows=cur.fetchall()
                for index in range(0,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_0.append(rows[index][1])
                for index in range(1,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_5.append(rows[index][1])
                for index in range(2,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_10.append(rows[index][1])
                for index in range(3,len(rows),4):
#                    print rows[index][0]
                    SENSOR_LIST_15.append(rows[index][1])
                v_star_0=np.median(np.array(SENSOR_LIST_0))
                v_star_5=np.median(np.array(SENSOR_LIST_5))
                v_star_10=np.median(np.array(SENSOR_LIST_10))
                v_star_15=np.median(np.array(SENSOR_LIST_15))
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time, sensorr, v_star_0])
#                print 'inserted ', v_star_0
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time+5, sensorr, v_star_5])
#                print 'inserted ', v_star_5
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time+10, sensorr, v_star_10])
#                print 'inserted ', v_star_10
                cur.execute("insert into vStarValsMedianWeek(minutes, sensorID, vStar) values (?, ?, ?)", [time+15, sensorr, v_star_15])
                print 'inserted ', v_star_15
            except:
                print 'could not find this i5 value at minute: ', time
                pass
        con.commit() 
    for row in cur.execute("select count(*) from vStarValsMedianWeek"):
        print row #before: 6365=67 sensors*95 values per sensor
    con.commit()   
if __name__ == '__main__':
    main()
    
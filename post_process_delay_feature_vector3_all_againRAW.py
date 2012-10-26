#!/usr/bin/python2.7
"""Author: Mahalia Miller
Date: September 2, 2001
Project: Given an incident, what is its impact?
This code takes a feature vector of the following form:
<time_of_day(minutes), postmile_on_i5, description_code, num_lanes, occ_at_start, 
v_at_start, v*_at_start-v_at_start, cum_incident_delay, cum_total_delay, 
police_duration, tot_duration, hit_another_incident_impact_region>
Then it creates a feature vector of the following form:
<time_of_day(minutes), rush_hour_or_not, postmile_on_i5, description_code, num_lanes, occ_at_start, 
v_at_start, v*_at_start-v_at_start, cum_incident_delay, cum_total_delay, 
police_duration, tot_duration, hit_another_incident_impact_region>
It also reduces the number of accident descriptions to 9 by grouping. Delays and durations are binned.
Results are outputted to another text file
"""
import numpy as np
from scipy.stats.mstats import mquantiles
import string, sys, csv, time
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
def process_accidents(rows):
    ''' creates file #list of minutes, postmile, date for incidents in test region'''
    lines=[]
    for row in rows:
        strings=string.split(row,'\n')[1:]
        for stringI in strings:
            tokens=string.split(stringI,'\t')
#            print tokens
            if len(tokens)>1:
#                if tokens[3]=='SR101-S':
                if tokens[3]=='I5-S':
                    try:
                        if float(tokens[6])<150:
#                        if (float(tokens[5])<410) and (float(tokens[5])>400):
                            dateTime=tokens[4]
#                            dateTime=tokens[1]
                            ww_0=time.strptime(dateTime, "%m/%d/%Y %H:%M")
#                            ww_0=time.strptime(dateTime, "%m/%d/%y %H:%M")

                            hours=time.strftime("%H", ww_0)
                            mins=time.strftime("%M", ww_0)
                            date=time.strftime("%m/%d/%Y", ww_0)
                            minutes_since_midnight=int(int(hours)*60+int(mins))
                            lines.append([minutes_since_midnight, float(tokens[6]), date])
                    except ValueError:
                        print 'BAAAAD', tokens
    return lines
def process_weather(rows):
    """Does following post-processing:
        - Transforms date
        - Transforms time of day
    Args:
        rows: a long string with some \n that indicate new incidents

    Returns:
        lines:
    """
    lines = [] #rain_in_hour_starting_at_this_time
    rainy_days=[]
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
                    print 'rain error tokens: ', tokens
#                    print tokens  
                date=time.strftime("%m/%d/%Y", ww)
                rain=float(tokens[3])
                lines.append([date, hours, rain])
                if rain>0.01:
                    rainy_days.append(date)
    return lines, rainy_days
def process_raw_incident_reports(rows):
    """Trys to find reports of:
    -# vehicles involved
    -whether or not lanes are reported as blocked
    -if tow truck was reported
    -if officers arrived on scene and how many
    -if a truck was reported as involved
 
    """
    lines = []
    for row in rows:
        strings=string.split(row,'\n')[1:]
        #initialize variables for the entire incident (will get re-initialized at each section divider)
        num_vs=0
        blocked_lanes=0
        tow_trucks=0
        clear_boolean=0
        officers=0
        truck_boolean=0
        date='NA'
        minutes_since_midnight='NA'
        for stringI in strings:
            tokens=string.split(stringI,'\t')
            #initialize variables that will change for each line
            num_vs_basic=0
            num_vs_alt=0
            blocked_lanes_basic=0
            blocked_lanes_alt=0
            if len(tokens)==1:
                if tokens[0]=='--': #hit the section divider
                    lines.append([date, minutes_since_midnight, num_vs, blocked_lanes, tow_trucks, clear_boolean, officers, truck_boolean])
                    num_vs=0
                    blocked_lanes=0
                    tow_trucks=0
                    clear_boolean=0
                    officers=0
                    truck_boolean=0
                elif tokens[0]=='Detailed data available for this incident':
                    continue
                else:
                    try:
                        dateTime=str(tokens[0]);
                        ww_0=time.strptime(dateTime, "%m/%d/%y %H:%M")
                        hours=time.strftime("%H", ww_0)
                        mins=time.strftime("%M", ww_0)
                        date=time.strftime("%m/%d/%Y", ww_0)
                        minutes_since_midnight=int(int(hours)*60+int(mins))
                    except:
                        continue
            if len(tokens)>1:
                #get date and time
                if tokens[0]=='Detail Id':
                    continue
                else:
                    dateTime=str(tokens[1]);
                    ww=time.strptime(dateTime, "%m/%d/%Y %H:%M:%S")
                    rep_hours=time.strftime("%H", ww)
                    rep_mins=time.strftime("%M", ww)
                    rep_minutes_since_midnight=int(int(rep_hours)*60+int(rep_mins))
                    delta_min=rep_minutes_since_midnight-minutes_since_midnight
                    if delta_min<-1400:
                        delta_min=1440+delta_min #assume there's an error
                if delta_min<2 and delta_min>-15:    
                    #check for vehicles
                    description=tokens[2]
                    num_vs_basic=description.count(' VS ')
                    if num_vs_basic>0:
                        num_vs_basic=num_vs_basic+1
                    if max(description.count('2 VEHS'), description.count('2 vehicles'), description.count('BOTH VEHS'), description.count('2 VEH'), description.count('VEH AND'), description.count('VEHS'))>0:
                        num_vs_alt=2
                    if max(description.count('3 VEHS'), description.count('3 vehicles'), description.count('3 VEH'), description.count('MULTI CAR'))>0:
                        num_vs_alt=3
                    num_vs=max((num_vs_basic), num_vs_alt, num_vs) #NOTE: if this is 0, but there is a truck, then it gets updated to 1 below
                    #check for blocked lanes
                    blocked_lanes_basic=description.count('#')
                    if max(description.count('BLKING'), description.count('BLK'), description.count('BLKG'), description.count('BLKD'), description.count('BLOCKED'), description.count('BLKED'))>0:
                        blocked_lanes_alt=1
                    blocked_lanes=max((blocked_lanes_basic), blocked_lanes_alt, blocked_lanes)    
                    if blocked_lanes>0:
                        blocked_lanes=1
                    #check for tow truck
                    if 'TOW' in description:
                        tow_trucks=1
                    #check if roadway clear
                    if max(description.count('RDWY CLR'), description.count('RDWY CLEAR'),clear_boolean)>0:
                        clear_boolean=1
                    #check for number of officers on scene
                    if 'Unit On Scene' in description:
                        officers=officers+1
                    #check if involves a truck
                    if max(description.count('TK'), description.count('SEMI'),description.count('BIG RIG'), truck_boolean)>0:
                        truck_boolean=1
                        num_vs=max(num_vs,1) #a truck counts as a vehicle 
        return lines
def process(rows, summarized_incident_list, raw_incident_lines, rain_lines, rainy_days):
    """Does following post-processing:
        - Condenses types to 9 types
        - Determines if in rush hour or not
        - Bins delays and durations and has values of 0 or 1 (0 is low, 1 is high)

    Args:
        rows: a long string with some \n that indicate new incidents

    Returns:


    Possible descriptions:
    -1013 - Weather Conditions
    -1125 - Traffic Hazard
    -1125A - Traffic Hazard - Animal
    -1125A - Traffic Hazard - Loose Animal
    -1125C - Traffic Hazard - Vehicle in Center Divider
    -1125CD - Traffic Hazard
    -1125CD - Traffic Hazard (Center Divider)
    -1125D - Traffic Hazard - Debris/Object
    -1125D - Traffic Hazard - Debris/Object in Road
    -1125D - Traffic Hazard - Debris/Objects
    -1125R - Roadway Hazard - Moving
    -1125V - Traffic Hazard - Vehicle
    -1126 - Disabled Vehicle
    -? 1144 - Possible Fatality
    1166 - Defective Traffic Signals
    -1179 - Collision - Ambulance Responding
    -1179 - Traffic Collision - Ambulance Responding
    -1180 - Traffic Collision - Major Injuries
    -1181 - Traffic Collision - Minor Injuries
    -1182 - Collision - Non Injury
    -1182 - Traffic Collision - No Injuries
    -1182 - Traffic Collision - Property Damage
    -1182H - Collision - Non Injury - Blocking Lane
    -1183 - Collision - No Further Details
    -1183 - Traffic Collision - No Details
    -20001 - Hit and Run - Injuries
    -20002 - Hit and Run - No Injuries
    3A - Request for 3A Tow
    -ANIMAL - Animal on Road
    BREAK - Request for Traffic Break
    BREAK - Traffic Break Requested
    -C/FIRE - Vehicle Fire
    -C/FIRE - Vehicle on Fire
    -CLOSUR - Road Closure
    -CLOSURE - Lane Closure
    -FIRE - Report of Fire
    -FIRE - Structure or Grass Fire
    -FLOOD - Roadway Flooding
    JUMPER - Attempt Suicide - Jumping from Bridge
    -PED - Ped
    -PED - Pedestrian on a Highway
    -PED - Pedestrian on the Roadway
    -SIGALERT - SigAlert - Lane Closure
    -SLIDE - Mud, Dirt or Rock Slide
    -? SPILL - Cargo or Hazardous Material Spill
    SPINOUT - Spinout
    T/ADV - Traffic Advisory
    -W/ADV - Wind Advisory
    W/WAY - Wrong Way Driver
    """
    lines = []
    d_inc_list=[]
    for row in rows:
        strings=string.split(row,'\n')[1:]
        for stringI in strings:
            tokens=string.split(stringI,'\t')
            print tokens
            if len(tokens)>1:
                #find if in rush hour or not
                minutes=float(tokens[0])
                if (minutes>=360 and minutes<=600) or (minutes>=900 and minutes<=1140):
                    rush_hour=str(1) #in rush hour!
                else:
                    rush_hour=str(0)
                #condense types
                typID=string.split(tokens[2], ' - ')[0]
                if typID in ['1125', '1125V', '1125A', '1125C', '1125D', '1125CD', '1125R', '1126', 'ANIMAL', 'PED', 'SPILL']:
                    typ='Traffic Hazard'  
                elif typID in ['1181','1182', '1182H']:
                    typ='Collision + no/minor injuries'
                elif typID in ['W/ADV', 'SLIDE', 'FLOOD','1013' ]:
                    typ='Natural weather hazard'
                elif typID in ['CLOSUR', 'CLOSURE', 'SIGALERT']:
                    typ='Lane closure'
                elif typID in ['FIRE', 'C/FIRE']:
                    typ='Fire'
                elif typID in ['1180', '1179','1144']:
                    typ='Collision + major injuries/ambulance' 
                elif typID in ['1183']:
                    typ= 'Collision - no details'  
                elif typID in ['20001','20002']:
                    typ='Hit and run' 
                else:
                    print typ
                    typ='Other'
                for min, pm, date in summarized_incident_list:
                    if int(min)==int(minutes) and float(tokens[1])==pm:
                        the_date=date
#                    else:
#                        print min
#                        print minutes
#                        print tokens[1]
                for start_date, start_time, numveh,blocked, tow, cleared, cops, trucks in raw_incident_lines:
                    if start_date==the_date and int(start_time)==int(minutes):
                        extras=[numveh, blocked, tow, cleared, cops, trucks]
                #update numveh if know there is a collision
                if extras[0]==0:
                    if typ=='Collision + no/minor injuries' or typ=='Collision - no details' or typ=='Collision + major injuries/ambulance' or typ=='Hit and run':
                        extras[0]=1
#                        print 'added number of vehicles'
                #get rain
                rain=get_init_rain(int(minutes), the_date, rain_lines)
                tokens[3]=rain
                if the_date in rainy_days:
                    tokens[4]=1
                else:
                    tokens[4]=0
                lines.append([tokens[0], rush_hour, tokens[1],typ]+extras+tokens[3:] )   
                if float(tokens[24])<3000:
                    d_inc_list.append(float(tokens[24]))
    boundaries=mquantiles(np.array(d_inc_list), [0.25, 0.5, 0.75])
    print '25, 50, 75 quantiles: ', boundaries
    boundaries=mquantiles(np.array(d_inc_list), [0.33, 0.66])
    print '33, 66 quantiles: ',boundaries
    return lines, 47.21, 65.69, 10, 20
def bucket(lines, d_inc_50, d_tot_50, pol_dur_50, tot_dur_50):
    '''buckets delay and incident duration'''
    binned_lines=[]
    for incident in lines:
        if float(incident[31])<d_inc_50:
            d_inc_b=0
        else:
            d_inc_b=1
        if float(incident[32])<d_tot_50:
            d_tot_b=0
        else:
            d_tot_b=1
        if float(incident[33])<pol_dur_50:
            pol_dur_d=0
        else:
            pol_dur_d=1
#        if float(incident[26])<tot_dur_50:
#            tot_dur_d=0
#        else:
#            tot_dur_d=1
        if float(incident[34])<15:
            tot_dur_d=0
        else:
            tot_dur_d=1
#        if float(incident[24])<240: #TODO: update bucket boundary
#            d_inc_b=0
#        elif float(incident[24])<1300:#TODO: update bucket boundary
#            d_inc_b=1
##        elif float(incident[23])<1400:#TODO: update bucket boundary
##            d_inc_b=2
#        else:
#            d_inc_b=
        if float(incident[31])<3000:
            try:
                binned_lines.append(incident[0:31]+[float(incident[22])-float(incident[13])]+[float(incident[23])-float(incident[14])]+[float(incident[24])-float(incident[15])]+incident[31:36])        
            except ValueError:
                print 'bad incident ', incident
                binned_lines.append(incident[0:31]+[-1]+[-1]+[-1]+incident[31:36])                        
    return binned_lines

def write_to_csv(lines_to_write, filename):
    spamWriter = csv.writer(open(filename, 'wb'))
    spamWriter.writerow(['time_of_day(minutes)', 'rush_hour',  'postmile', 'description', 'numvehs', 'blocked', 'tow', 'cleared', 'cops', 'truck', 'rain', 'rainy_day','num_lanes', 'occ_b0', 'v_b0', 'v_diff_b0', 'occ_b5', 'v_b5', 'v_diff_b5','occ_b10', 'v_b10', 'v_diff_b10','occ_c0', 'v_c0', 'v_diff_c0','occ_c5', 'v_c5', 'v_diff_c5','occ_c10', 'v_c10', 'v_diff_c10','occ_c0-occ_b0','v_c0-v_b0', 'v_diff_c0-v_diff_b0','cum_incident_delay', 'cum_total_delay', 'police_duration', 'tot_duration', 'adjacent'])
    for line in lines_to_write:
        spamWriter.writerow(line)
    pass
def get_init_rain(min, date, rain_list):
    hour=int(min/60.0)
    for rain_date, rain_hour, rain in rain_list:
        if rain_date==date and rain_hour==hour:
            return rain
    return 0  
def main():
    rows=run_on_file('jan_feb_incidents_full.txt') #details
#    rows=run_on_file('julaugIncidentDetails.txt') #details

    incident_lines=process_raw_incident_reports(rows)
    print 'incident lines: ', incident_lines
    incident_rows=run_on_file('accident_details_janfeb2009.txt') #summaries
#    incident_rows=run_on_file('julaugIncidentSummraies.txt') #summaries
    print 'incident_rows', incident_rows
    summarized_incident_list=process_accidents(incident_rows) #list of minutes, postmile, date for incidents in test region
    print 'summarized', summarized_incident_list
    print 'why are postmile so long and weird???????'
    rows=run_on_file('wwp_accumulated_rain.txt')
    rain_lines, rainy_days=process_weather(rows)
    print rainy_days
#    asdf=1/0.0
#    rows=run_on_file('20110910_nosize.txt') #raw feature vector
    rows=run_on_file('20111010_featureVector_d7v3.txt') #raw feature vector
    lines, d_inc_50, d_tot_50, pol_dur_50, tot_dur_50=process(rows, summarized_incident_list, incident_lines, rain_lines, rainy_days)
    starters=set()
    edited_lines=[]
    for line in lines:
        if tuple([line[0], line[2]]) not in starters:
            starters.add(tuple([line[0], line[2]]))
            edited_lines.append(line)
    print 'Number of duplicates: ', len(lines)-len(edited_lines)
#    print len(edited_lines)
    binned_lines=bucket(edited_lines,d_inc_50, d_tot_50, pol_dur_50, tot_dur_50)
    write_to_csv(binned_lines, '20111010_featurevectorLA_postprocessed.csv')
    print 'Writing to files complete'
if __name__ == '__main__':
    main()
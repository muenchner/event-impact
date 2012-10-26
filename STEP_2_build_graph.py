#!/usr/bin/python2.7
"""Author: Mahalia Miller
Date: August 10, 2011
Builds graph of D7 sensors
"""
from networkx import *
import matplotlib.pyplot as plt
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
        
           
def find_all_connectors(rows):
    """Tokenizes string and returns list of sensors that connect corridors.

    Args:
        rows: a long string with some \n that indicate sensors
        minPM: postmile as int
        maxPM: postmile as int
        fwy: for example, 5
    Returns:
        lines: list of ids in corridor with id, postmile, direction, coordinates (list), and highway as a list
                e.g. 715898, 117.28, 'S', ['33.880183', '-118.021787'], 5]
        short_lines: list of targeted info, namely:
                    <source route, source direction, [lat, lon], destination route, destination direction>
    """
    lines = []
    short_lines=[]
    counter=0
    road_list=['FF']
    for row in rows:
        strings=string.split(row,'\n')[1:]
        for stringI in strings:
            tokens=string.split(stringI,'\t')
            if len(tokens)>1:
                if tokens[11] in road_list:
                    description=tokens[13]
                    clues=string.split(description, ' ')
                    for c in clues:
                        if c=='to':
                            pieces=string.split(description, 'to')
                        elif c=='TO':
                            pieces=string.split(description, 'TO')
                        else:
                            continue
                    i=string.split(pieces[0], ' ')
                    j=string.split(pieces[1], ' ')
                    road_i=0
                    road_j=0
                    dir_i=None
                    dir_j=None
                    dir_i, road_i=handle_special_cases(i)
                    dir_j, road_j=handle_special_cases(j)
                    if dir_i==None or road_i==None:
                        for s in i:
                            try:
                                road_i=int(s)
                            except ValueError:
                                dir_i=parse_direction(s, dir_i)
                    if dir_j==None or road_j==None:
                        for t in j:
                            try:
                                road_j=int(t)
                            except ValueError:
                                dir_j=parse_direction(t, dir_j)
                    if dir_i==None or dir_j==None or road_i==0 or road_j==0:
                        if dir_i==None:
                            print 'bad i in : ', description
                        if dir_j==None:
                            print 'bad j in : ', description
                        if (road_i==0 or road_j==0):
                            print 'unhandled road case: ', description                        
                    else:
                        
#                        print stringI
                        if tokens[2]==dir_j:
                            counter=counter+1
                            lines.append([int(tokens[0]), float(tokens[7]), tokens[2], [float(tokens[8]), float(tokens[9])], int(tokens[1])])
                            short_lines.append([road_i, dir_i, [float(tokens[8]), float(tokens[9])], road_j, dir_j])
#                            print 'connecting from road '+str(road_i)+' in direction '+dir_i+' to '+str(road_j)+' in direction '+dir_j+' near postmile '+tokens[7]   
                        elif tokens[2]==dir_i:
                            counter=counter+1
                            lines.append([int(tokens[0]), float(tokens[7]), tokens[2], [float(tokens[8]), float(tokens[9])], int(tokens[1])])
                            short_lines.append([road_i, dir_i, [float(tokens[8]), float(tokens[9])], road_j, dir_j])
#                            print 'connecting from road '+str(road_i)+' in direction '+dir_i+' near postmile '+tokens[7]+' to '+str(road_j)+' in direction '+dir_j   
                        else:
                            print 'WEIRD DIRECTION: ', description
                            continue       
                        #print 'dir_i: ', dir_i
#                    print 'dir_j: ', dir_j


    return lines, short_lines

def add_nodes(list_of_ids, G, singleGraph):
    """Adds nodes to a graph and returns list of roads"""
    road_set=set()
    for id, pm, dir, coords, hwy in list_of_ids:
        id_dict=dict(lat=coords[0], lon=coords[1], dire=dir, mile=pm, road=hwy)
        G.add_node(id, id_dict)
        singleGraph.add_node(id)
        singleGraph.position[id]=(coords[1], coords[0])
        road_set.add(int(hwy))
    print 'road set: ', road_set
    return road_set, G, singleGraph    
def add_corridor_edges(G, road_set, irrationals, singleGraph):
    """adds edges for each corridor"""
    road_dict = {}
    for road in road_set:
        sensor_rows = run_on_file('d07_stations_2008_11_26.txt')
        if road not in irrationals:
            if road%2==0:
                dir1='E'#for even numbered roads, east is ascending
                dir2='W'
            else:
                dir1='N'#for odd numbered roads, north is ascending
                dir2='S'
        else:
            if road%2==0:
                dir1='N'#for most even numbered roads, east is ascending
                dir2='S'
            else:
                dir1='E'#for most odd numbered roads, north is ascending
                dir2='W'
        id_list_1=find_ids(sensor_rows, 0, 100000000, road, dir1)
        sensor_rows = run_on_file('d07_stations_2008_11_26.txt')
        id_list_2=find_ids(sensor_rows,0,100000000, road, dir2)
        road_dict[(road, dir1)]=id_list_1
        road_dict[(road, dir2)]=id_list_2
        sorted_list_1=sorted(id_list_1, key=itemgetter(1))
        sorted_list_2=sorted(id_list_2, key=itemgetter(1), reverse=True)
        if len(sorted_list_1)<2:
            print 'bad road', road
        for i in range(0, len(sorted_list_1)-1):
            G.add_edge(sorted_list_1[i][0], sorted_list_1[i+1][0])
            singleGraph.add_edge(sorted_list_1[i][0], sorted_list_1[i+1][0])
        for i in range(0, len(sorted_list_2)-1):
            G.add_edge(sorted_list_2[i][0], sorted_list_2[i+1][0])  
            singleGraph.add_edge(sorted_list_2[i][0], sorted_list_2[i+1][0])      
    print road_dict
    return G, road_dict, singleGraph
def manual_connections(G,singleGraph,ml_dict):
    '''Adds some edges manually that are known problems'''
    #TODO: make this file happen
    extra_lines=[]
#    extra_lines.append([605,'N', ?,?,5,'N'], [5,'S', ?,?,605,'N'],[5,'S', ?,?,605,'S'])
#    for road_i, dir_i, coords, road_j, dir_j in extra_lines:
#        source_list=ml_dict[(road_i, dir_i)]
#        destination_list=ml_dict[(road_j, dir_j)]
#        source_line=nearest(coords, source_list, 1)
#        destination_line=nearest(coords, destination_list, 1)
#        G.add_edge(source_line[0][1][0], destination_line[0][1][0])        
#        singleGraph.add_edge(source_line[0][1][0], destination_line[0][1][0]) 
    return G, singleGraph
def connect_corridors(G, all_ml_list, ml_dict, singleGraph):
    """Adds edges between corridors"""
    sensor_rows = run_on_file('d07_stations_2008_11_26.txt')
    connector_list, short_lines=find_all_connectors(sensor_rows)
    print 'number of connectors parsed in dataset: ', len(connector_list)
    for road_i, dir_i, coords, road_j, dir_j in short_lines:
        source_list=ml_dict[(road_i, dir_i)]
        destination_list=ml_dict[(road_j, dir_j)]
        source_line=nearest(coords, source_list, 1)
        destination_line=nearest(coords, destination_list, 1)
        G.add_edge(source_line[0][1][0], destination_line[0][1][0])        
        singleGraph.add_edge(source_line[0][1][0], destination_line[0][1][0])        
    G, singleGraph=manual_connections(G, singleGraph, ml_dict)   
    return G, singleGraph

def plot_graph(output_plot_filename, xlima, xlimb, ylima, ylimb, singleGraph):
    #plot
    try:
        node_color=[float(singleGraph.degree(v)) for v in singleGraph]
        node_size=[5.0*(float(singleGraph.degree(v))) for v in singleGraph]
        nx.draw(singleGraph,singleGraph.position,node_color=node_color, node_size=node_size, width=4,alpha=0.4,edge_color='0.75',font_size=0)
        plt.xlim(xlima,xlimb)
        plt.ylim(ylima,ylimb)
        plt.savefig(output_plot_filename)

        plt.show() #consider instead: http://networkx.lanl.gov/examples/graph/napoleon_russian_campaign.html
    except:
        print'error found when plotting'
        pass
def main(sensor_filename, irrationals, output_graph_filename, output_plot_filename, xlima, xlimb, ylima, ylimb):
    #initialize graph
    DG=nx.DiGraph()
    singleGraph=nx.Graph()
    singleGraph.position={}
    
    #add nodes
    sensor_rows = run_on_file(sensor_filename)
    id_det_list=find_all_ids(sensor_rows)
    road_set, DG, singleGraph=add_nodes(id_det_list, DG, singleGraph)
    
    #add edges in the direction of traffic flow, NOT accidents
    #first, add on each corridor
    DG, road_dict, singleGraph=add_corridor_edges(DG, road_set, irrationals, singleGraph) 
    
    #second, connect corridors #Note: the FF sensors link corridors
    DG, singleGraph=connect_corridors(DG, id_det_list, road_dict, singleGraph)
    
    #pickle out graph
    nx.write_gpickle(singleGraph,output_graph_filename)
    print 'number of connected components in graph: ',number_connected_components(DG.to_undirected())
    return singleGraph
    
    
    
if __name__ == '__main__':
    #include parameters for Los Angeles Caltrans District 7
    sensor_filename = 'd07_stations_2008_11_26.txt'
    irrationals=[105,110,710,14,91,170]
    output_graph_filename = 'graphd07.gpickle'
    output_plot_filename = 'graphd07.pdf'
    xlima = -119.562
    xlimb = -117.508
    ylima = 33.6075
    ylimb = 34.795
    main(sensor_filename, irrationals, output_graph_filename, output_plot_filename, xlima, xlimb, ylima, ylimb)

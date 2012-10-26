'''
Author: Mahalia Miller
Ideas: Mahalia Miller and Chetan Gupta
Date: Summer 2011, updated August 22, 2012
'''

class PEMSConfig(object):
#this configuration class has all the file information needed for the main model in run_pems_model.py
    def __init__(self, district):
        '''
        Constructor
        '''
        #Los Angeles
        if int(district)==7:
            self.sensor_filename = 'd07_stations_2008_11_26.txt'
            self.irrationals=[105,110,710,14,91,170]
            self.output_graph_filename = 'graphd07.gpickle'
            self.output_plot_filename = 'graphd07.pdf'
            self.xlima = -119.562
            self.xlimb = -117.508
            self.ylima = 33.6075
            self.ylimb = 34.795
            self.rain_filename = 'daily_precipitation_d7.txt' 
            self.db_file = 'd7Huge.db'
            self.tables = ['sensors','vStarValsMedianWeek', 'incidents', 'devices']
            self.feature_vector_output_filename = 'd07_feature_vector.txt'
        #San Francisco Bay Area  
        elif int(district == 4):
            self.sensor_filename = 'd04_stations_2010_05_18.txt'
            self.irrationals=[]
            self.output_graph_filename = 'graphd04.gpickle'
            self.output_plot_filename = 'graphd04.pdf'
            self.xlima = -122.7
            self.xlimb = -121.7
            self.ylima = 37.3
            self.ylimb = 38.2
            self.rain_filename = 'daily_precipitation_d4.txt' 
            self.db_file = 'd4.db'
            self.tables = ['sensors','vStarValsMedianWeek', 'incidents', 'devices']
            self.feature_vector_output_filename = 'd04_feature_vector.txt'
        else:
            print 'Sorry. Please re-run again with a supported district number'
            

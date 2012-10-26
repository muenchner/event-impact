'''
Author: Mahalia Miller
Ideas: Mahalia Miller and Chetan Gupta
Date: Summer 2011, updated August 22, 2012
'''

from pems_config import PEMSConfig
import STEP_2_build_graph as step2
import STEP_3_populate_feature_vector as step3
import STEP4_post_process as step4

def main():
    #Prompt user to input a district ID where 4=San Francisco Bay Area, California, USA and 7-Los Angeles Area, California, USA
    district = float(raw_input('For which Caltrans district do you want to build a feature vector? 7 or 4? \n'))
    
    #call in settings by initializing class
    PEMS = PEMSConfig(district)
    
    #first step is to calculate the v* values. This is done by downloading data from pems.ca.gov and populating some databases.
    
    #second step is to create a graph of sensors and store it as a python pickle
    singleGraph = step2.main(PEMS.sensor_filename, PEMS.irrationals, PEMS.output_graph_filename, PEMS.output_plot_filename, PEMS.xlima, PEMS.xlimb, PEMS.ylima, PEMS.ylimb)
    
    #plot graph
    step2.plot_graph(PEMS.output_plot_filename, PEMS.xlima, PEMS.xlimb, PEMS.ylima, PEMS.ylimb, singleGraph)
    
    #third step is to create the raw feature vector 
    step3.main(singleGraph, PEMS.rain_filename, PEMS.db_file, PEMS.tables, PEMS.feature_vector_output_filename)
    
    #fourth step is to post-process the feature vector and write a final feature vector
    step4.main()
    
    #fifth step is to do machine learning on the feature vector such as in weka
    
    
if __name__ == '__main__':
    main()
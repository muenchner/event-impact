event-impact
============

This project aims to predict the impact class of a traffic incident based on initial sensor recordings. Project is in Python with SQLite.



To run the model, look at run_pems_model.py.

First step is to calculate the v* values. This is done by downloading data from pems.ca.gov and populating some databases.
    
Second step is to create a graph of sensors and store it as a python pickle

Third step is to create the raw feature vector 

Fourth step is to post-process the feature vector and write a final feature vector

Fifth step is to do machine learning on the feature vector such as in weka
    
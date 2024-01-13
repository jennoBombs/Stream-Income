#!/usr/bin/env python
# -*- coding: utf-8 -*-
__author__ = "Jennifer Reisinger"
__email__ = "jhubbard3@students.columbiabasin.edu"
__date__ = "Spring 2023"
__version__ = "0.0.1"

#libraries
import pandas as pd
from collections import defaultdict
import sys

#system arguments
script_name = sys.argv[0]
script_arguments = sys.argv[1:]
data_file = script_arguments[0]

#initialize variables
total_count = 0
unique_income_count = 0
mean = 0
median = 0
mode = 0

#streaming variables
median_list = []
mode_dict = defaultdict(int)

#print the header
print("Total Count,Unique Income Count,Mean,Median,Mode")

#read the file by chunks into pandas
for chunk in pd.read_csv(data_file,chunksize=100):
    
    #update the total and unique income count
    total_count += len(chunk)
    unique_income_count += chunk.iloc[:, 0].nunique()
    
    #update the mean
    mean = ((mean * (total_count - len(chunk))) + chunk.iloc[:, 0].sum()) / total_count
    
    #update the median
    chunk_median_list = chunk.iloc[:, 0].tolist()
    for value in chunk_median_list:
        #add the value to the list, sorts, and evaluates for new median
        median_list.append(value)
        median_list.sort()
        n = len(median_list)
        if n % 2 == 0:
            median = (median_list[n//2-1] + median_list[n//2])/2
        else:
            median = median_list[n//2]
    
    #update the mode
    for value in chunk.iloc[:, 0].tolist():
        mode_dict[value] += 1
    mode = max(mode_dict, key=mode_dict.get)
    
        
    #output results
    print(f"{total_count},{unique_income_count},{mean:.0f},{median:.0f},{mode}")
        
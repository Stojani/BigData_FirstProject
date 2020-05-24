#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 10 11:25:54 2020

@author: DataFast group
"""

"""
Un job che sia in grado di generarele statistiche di ciascuna azione tra il 
2008 e il 2018 indicando, per ogni azione: 
    (a)il simbolo,
    (b) la variazione della quotazione (differenza percentuale arrotondata tra 
    i prezzi di chiusura iniziale e finale dell’intervallo  temporale),  
    (c) il  prezzo  minimo, 
    (e) quello  massimo  e 
    (f) il  volume  medio nell’intervallo, 
    ordinando l’elenco inordine decrescente di variazionedella quotazione
"""

from pyspark import SparkConf, SparkContext, StorageLevel
import datetime
import time

size = "" # 50, 25

start_time = time.time()

# Functions
def isFirstDateAfterSecondDate(date_string_1, date_string_2): 
    d1 = datetime.datetime.strptime(date_string_1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(date_string_2, "%Y-%m-%d") 
    return d1 > d2

def calculateVariation(price_pair):
        end_price = float(price_pair[0][0])
        start_price = float(price_pair[1][0])
        percentage = ((end_price - start_price) / start_price ) * 100
        return percentage

sc = SparkContext(conf=SparkConf().setMaster("local[*]").setAppName("First Job"))


dataset = sc.textFile('file:///home/hadoop/prices'+size+'.csv')

# Record: 
# [
#   [0 'ticker', 1 'open', 2 'close', 3 'adj_close', 4 'low', 5 'high', 6 'volume',  7 'date'], 
#   ['AHH', '11.5', '11.5799999237061', '8.49315452575684', '11.25', '11.6800003051758', '4633900', '2013-05-08']
# ]

# Convert the dataset lines to list
formatted_rdd = dataset.map(lambda line: line.split(","))
    
# Skip first value
rdd_header = formatted_rdd.first()
formatted_rdd_values = formatted_rdd.filter(lambda x : x != rdd_header)    

# Take only from 2008 to 2018
rdd_year_filtered = formatted_rdd_values.filter(lambda x: int(x[7][:4]) > 2008) \
        .map(lambda x : (x[0], x[2], x[6], x[7])).persist(StorageLevel.MEMORY_AND_DISK)

# Rdd min close price
rdd_min_price = rdd_year_filtered.map(lambda x: (x[0], float(x[1]))).reduceByKey(lambda x, y : min(x,y))

# Rdd max close price
rdd_max_price = rdd_year_filtered.map(lambda x: (x[0], float(x[1]))).reduceByKey(lambda x, y : max(x,y))

# Rdd avg volume
rdd_avg_volume = rdd_year_filtered.map(lambda x: (x[0], (float(x[2]), 1))) \
        .reduceByKey(lambda x, y: ( x[0] + y[0], x[1] + y[1] ) ) \
        .map(lambda x: (x[0], x[1][0] / x[1][1]))

# Rdd volume difference
rdd_first_price = rdd_year_filtered.map(lambda x : (x[0], (x[1], x[3])) )\
        .reduceByKey(lambda x,y: (x[0],x[1]) if(isFirstDateAfterSecondDate(x[1], y[1]) == False) else (y[0], y[1]))

rdd_last_price = rdd_year_filtered.map(lambda x : (x[0], (x[1], x[3])) )\
        .reduceByKey(lambda x,y: (x[0],x[1]) if(isFirstDateAfterSecondDate(x[1], y[1]) == True) else (y[0], y[1]))

rdd_price_percentage_variance = rdd_last_price.join(rdd_first_price) \
        .map(lambda x: (x[0], calculateVariation(x[1])))

# Generate output
rdd_output = rdd_max_price.join(rdd_min_price).join(rdd_avg_volume).join(rdd_price_percentage_variance)

# Format output ['GTS', 51.641320105180014, 10.9399995803833, 21.25, 106292.70216962525]
rdd_output_format = rdd_output.map(lambda x : [x[0], x[1][1], x[1][0][0][1], x[1][0][0][0], x[1][0][1]]).sortBy(lambda x : x[1],  ascending=False)

rdd_output_format.coalesce(1).saveAsTextFile('./output/outJob1_25.txt')

# print('___OUTPUT =' + str(rdd_output_format.take(10)))
print("seconds: " + str(time.time() - start_time))
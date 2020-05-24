#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 14 21:30:35 2020

@author: DavideFarioli
"""

''' Un job che sia in grado di generare, per ciascun settore, il relativo “trend” nel periodo 2008-2018, 
ovvero un elenco contenete,  
per  ciascun  anno  nell’intervallo:  
    (a)  il  MEDIA di tutto il volume  delle  azioni del  settore,  
    (b)  la variazione annuale MEDIA delle aziende del settore 
    (c)  la quotazione giornaliera media delle aziende del settore. 

    .2 Pervariazione annuale di un’azione si intende la differenza percentuale tra la quotazione di fine anno e quella di inizio anno
'''

from pyspark import SparkConf, SparkContext, StorageLevel
import datetime

import time

size = '' # 50, 25

start_time = time.time()

# Functions
def isFirstDateAfterSecondDate(date_string_1, date_string_2): 
    d1 = datetime.datetime.strptime(date_string_1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(date_string_2, "%Y-%m-%d") 
    return d1 > d2

def calculateVariance(couple):
        end_price = float(couple[0][0])
        start_price = float(couple[1][0])
        percentage = ((end_price - start_price) / start_price ) * 100
        return percentage

sc = SparkContext(conf=SparkConf().setMaster("local[*]").setAppName("Second Job"))

dataset_sector_path = "file///../../samples/stocks.csv"
dataset_price_path = "file///../../samples/prices"+size+".csv"
# dataset_sector_path = 'file///home/hadoop/stocks'+size+'.csv'
# dataset_price_path = 'file///home/hadoop/prices.csv'

# Import and clean of stocks
stocksRDD = sc.textFile(dataset_sector_path, 2)
stocks_linesRDD = stocksRDD.map(lambda line: line.split(","))
stocks_first_line = stocks_linesRDD.first()
stocks_valuesRDD = stocks_linesRDD.filter(lambda x : x != stocks_first_line) 

# (ticker, sector)
stocks = stocks_valuesRDD.map(lambda x : (x[0], x[3]))

# Import and clean of prices
priceRDD = sc.textFile(dataset_price_path, 2)
price_linesRDD = priceRDD.map(lambda line: line.split(","))
price_header = price_linesRDD.first()
formatted_rdd_values = price_linesRDD.filter(lambda x : x != price_header) 
values_year_filtered = formatted_rdd_values.filter(lambda x : int(x[7][:4]) > 2008)

# ('AHH', ['11.5799999237061', '4633900', '2013-05-08'])
values = values_year_filtered.map(lambda x : (x[0], [x[2], x[6], x[7] ]))

# (('BIDU', '2009'), (' INC."', chiusura '13.5410003662109', volume '16551000', data '2009-01-02'))
dataset_rdd = stocks.join(values).map(lambda x : ( (x[1][0], x[1][1][2][:4]), (x[1][1][0], x[1][1][1], x[1][1][2]) )).persist(StorageLevel.MEMORY_AND_DISK)

# [('CPB', ('CONSUMER NON-DURABLES', ['30.4300003051758', '2050400', '2009-01-02']))]
# print("Dataset :" + str(dataset_rdd.take(5)))

# Rdd avg volume
rdd_avg_volume = dataset_rdd.map(lambda x : (x[0], (float(x[1][1]), 1))).reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])).map(lambda x: (x[0], (x[1][0]/x[1][1])))

# Rdd avg variation
rdd_first_price = dataset_rdd.map(lambda x : (x[0], (x[1][0], x[1][2])) ).reduceByKey(lambda x,y: (x[0],x[1]) if(isFirstDateAfterSecondDate(x[1], y[1]) == False) else (y[0], y[1]))

rdd_last_price = dataset_rdd.map(lambda x : (x[0], (x[1][0], x[1][2])) ).reduceByKey(lambda x,y: (x[0],x[1]) if(isFirstDateAfterSecondDate(x[1], y[1]) == True) else (y[0], y[1]))

rdd_price_percentage_variation = rdd_last_price.join(rdd_first_price)\
    .map(lambda x: (x[0], calculateVariance(x[1])))

# Rdd avg quotation
rdd_avg_quotation = dataset_rdd.map(lambda x : (x[0], (float(x[1][0]), 1)))\
    .reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))\
    .map(lambda x: (x[0], (x[1][0]/x[1][1])))

# format output
rdd_output = rdd_avg_volume.join(rdd_price_percentage_variation).join(rdd_avg_quotation)\
    .map(lambda x : [x[0][0], x[0][1], x[1][0][0], x[1][0][1], x[1][1]])
print('Results' + str(rdd_output.take(10)))


#rdd_output_format.coalesce(1).saveAsTextFile('./output/resultsJob2_0_'+size+'.txt')
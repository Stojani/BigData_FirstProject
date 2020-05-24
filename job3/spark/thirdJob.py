#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''

@author: DataFast Group

Un job in grado di generare gruppi di aziende le cui azioni hanno avuto lo stesso trend 
in termini di variazione annuale nell’ultimo triennio disponibile, indicando le aziende e il trend comune 
(es. {Apple, Intel, Amazon}:2016:-1%, 2017:+3%, 2018:+5%).

Variazione annuale di un’azione si intendela differenza percentuale tra la quotazione di fine anno e quella di inizio anno
'''

from pyspark import SparkConf, SparkContext, StorageLevel
import datetime
import time

start_time = time.time()

size = "" # 50, 25

def mapDatasetValues(el):
    key = (el[1][0], el[1][1][1][:4])
    value = (el[1][1][0], el[1][1][1])
    return (key, value)
    
def isFirstDateAfterSecondDate(date_string_1, date_string_2): 
    d1 = datetime.datetime.strptime(date_string_1, "%Y-%m-%d")
    d2 = datetime.datetime.strptime(date_string_2, "%Y-%m-%d") 
    return d1 > d2

def calculatePercentageVariation(couple):
    end_price = float(couple[0][0])
    start_price = float(couple[1][0])
    percentage = ((end_price - start_price) / start_price ) * 100
    return round(percentage, 2)

def sortYearPercentageVariationList(years): 
      
    # Insert arr[1] 
    if (float(years[1][0]) < float(years[0][0])): 
        years[0], years[1] = years[1], years[0] 
          
    # Insert years[2] 
    if (float(years[2][0]) < float(years[1][0])): 
        years[1], years[2] = years[2], years[1] 
        if (float(years[1][0]) < float(years[0][0])): 
            years[1], years[0] = years[0], years[1] 
    return years

# Create spark context
sc = SparkContext(conf=SparkConf().setMaster("local[*]").setAppName("Third Job"))

dataset_price_path = 'file:///home/hadoop/prices'+size+'.csv'
dataset_stocks_path = 'file:///home/hadoop/stocks.csv'

# Import and clean of stocks
rdd_stocks = sc.textFile(dataset_stocks_path, 2)
rdd_stocks_line = rdd_stocks.map(lambda line: line.split(","))

stocks_header = rdd_stocks_line.first()
stocks_valuesRDD = rdd_stocks_line.filter(lambda x : x != stocks_header) 
# ticker, name
stocks = stocks_valuesRDD.map(lambda x : (x[0], x[2]))

# Import and clean of prices
rdd_price = sc.textFile(dataset_price_path, 2)
rdd_price_lines = rdd_price.map(lambda line: line.split(","))

price_header = rdd_price_lines.first()
formatted_rdd_values = rdd_price_lines.filter(lambda x : x != price_header) 

# Filters values taking only data from 2016
values_year_filtered = formatted_rdd_values.filter(lambda x : int(x[7][:4]) > 2015)

# Format values
values = values_year_filtered.map(lambda x : (x[0], [ x[2], x[7] ]))

# [('GTT', ('"GTT COMMUNICATIONS', ['29.0499992370605', '2017-02-16'])) => [('azienda', 'year'), ('close', 'date')]
dataset_rdd = stocks.join(values).map(lambda x : mapDatasetValues(x))

# Annual Variation
rdd_year_first_price = dataset_rdd.map(lambda x : (x[0], (x[1][0], x[1][1])) )\
        .reduceByKey(lambda x,y: (x[0],x[1]) if(isFirstDateAfterSecondDate(x[1], y[1]) == False) else (y[0], y[1]))

rdd_year_last_price = dataset_rdd.map(lambda x : (x[0], (x[1][0], x[1][1])) )\
        .reduceByKey(lambda x,y: (x[0],x[1]) if(isFirstDateAfterSecondDate(x[1], y[1]) == True) else (y[0], y[1]))

# es. (('8X8 INC', '2018'), 57.638898363084245) , [('azienda', 'year'), annual_variation]
rdd_price_percentage_variance = rdd_year_last_price.join(rdd_year_first_price) \
    .map(lambda x: (x[0], calculatePercentageVariation(x[1])))

# ('8X8 INC', [('2018', 57.638898363084245), ('2016', 32.284917998568865), ('2017', -3.4246574447669147)])
# [('"SYNLOGIC', [('2016', -71.01449214951533), ('2017', -23.0159526087921), ('2018', -16.203703417536328)]), ('"MYND ANALYTICS', [('2016', 50.83333651224766), ('2017', -63.243242212244), ('2018', -54.59940583541164)]), ('"MERRILL LYNCH DEPOSITOR', [('2016', -0.7310524652979797), ('2017', -42.22309282360425), ('2018', 15.052263344893916)]), ('"TAUBMAN CENTERS', [('2016', -1.6365131679120615), ('2017', -12.020979038913756), ('2018', -2.214190742909241)]), ('"ASPEN AEROGELS', [('2016', -31.735537450631522), ('2017', 14.823532104492228), ('2018', -3.815262178569402)])]

# Take only the companies with 3 years and prepare the key { (trend2016, trend2017, trend2018), company }
rdd_ordered_results = rdd_price_percentage_variance.map(lambda x : (x[0][0], ( x[0][1], x[1] ) ))\
    .groupByKey().filter(lambda x : len(x[1]) > 2).map(lambda x : (x[0], sortYearPercentageVariationList(list(x[1]))))

# Format rdd_output => (trend2016, trend2017, trend2018) [list of companies]
rdd_output = rdd_ordered_results.map( lambda x: ((x[1][0][1], x[1][1][1], x[1][2][1]), x[0]) ).groupByKey()\
    .map(lambda x : (x[0], list(x[1])))

# Print results to text file
rdd_output.coalesce(1).saveAsTextFile('./output/resultsJob3_'+size+'.txt')
#print(rdd_output.take(10))
print("seconds: " + str(time.time() - start_time))
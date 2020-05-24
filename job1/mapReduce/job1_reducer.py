#!/usr/bin/python3

import sys
from datetime import datetime as dt
from operator import itemgetter

# historical_stock_prices.csv : fields = [ticker, open, close, adj_close, low, high, volume, date]
#                                        [0     , 1   , 2    , 3        , 4  , 5   , 6     , 7   ]
# historical_stocks.csv : fields = "ticker, exchange, name, sector, industry"


# mapper sample output: 
"""
AHH	9.97999954223633	17800	2014-04-21
"""
#(ticker, close, volume, date)

resultList = []
tickers_dict = {}

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    fields = line.split('\t')   
    #ticker,close,volume,date = fields
    ticker = fields[0]
    close = float(fields[1])
    low_close = float(fields[1])
    high_close = float(fields[1])
    volume = int(fields[2])
    date = dt.strptime(fields[3], '%Y-%m-%d').date()

    if(ticker not in tickers_dict):
        tickers_dict[ticker] = [low_close,high_close,volume,1,date,close,date,close]
    
    else:
        # volume incrementing
        tickers_dict[ticker][2] +=volume
        tickers_dict[ticker][3] +=1

         # searching min low_close price
        if(low_close < tickers_dict[ticker][0]):
            tickers_dict[ticker][0] = low_close
        
        # searching max high_close price
        if(high_close > tickers_dict[ticker][1]):
            tickers_dict[ticker][1] = high_close
        
        # searching initial close price
        if(date < tickers_dict[ticker][4]):
            tickers_dict[ticker][4] = date
            tickers_dict[ticker][5] = close

        # searching final close price  
        elif(date > tickers_dict[ticker][6]):
            tickers_dict[ticker][6] = date
            tickers_dict[ticker][7] = close

for ticker in tickers_dict.keys():

    # calculate variation of ticker
    close_final = tickers_dict.get(ticker)[7]
    close_initial = tickers_dict.get(ticker)[5]    
    variation = round((close_final/close_initial)*100 - 100)

    # calculate average volume of ticker
    volumeSum = tickers_dict.get(ticker)[2]
    volumeCount = tickers_dict.get(ticker)[3]
    volumeMean = volumeSum / volumeCount

    # get min low and max high_close price of ticker
    min_low = tickers_dict.get(ticker)[0]
    max_high = tickers_dict.get(ticker)[1]

    resultList.append([ticker,variation,min_low,max_high,volumeMean])

# sort list by variation
result = sorted(resultList, key=itemgetter(1), reverse=True)
#print(result)
# print top 10 rows of result
for i in range(10):
    #print(result[i])
    print('{}\t{}%\t{}\t{}\t{}'.format(result[i][0],result[i][1],result[i][2],result[i][3],result[i][4]))

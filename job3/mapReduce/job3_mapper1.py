#!/usr/bin/python3

import sys


for line in sys.stdin:

    # split the line into fields
    fields = line.strip().split(',')

    try:
        if(len(fields) == 8):
            # condition verified if fields are from historical_stock_prices.csv file

            ticker,open,close,adj_close,low,high,volume,date = fields

            year = int(date[0:4])

            if((year >= 2016) and (year <= 2018)):

                print('{}\t{}\t{}\t-'.format(ticker,close,date))
        else:
            # fields are from historical_stocks.csv file
            ticker,exchange,name,sector,industry = fields

            print('{}\t-\t-\t{}'.format(ticker,name))

    except:
        # skip line
        continue
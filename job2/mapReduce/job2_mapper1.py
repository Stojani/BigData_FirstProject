#!/usr/bin/python3

import sys

# input dataset:
# historical_stock_prices.csv : fields = [ticker, open, close, adj_close, low, high, volume, date]
#                                        [0     , 1   , 2    , 3        , 4  , 5   , 6     , 7   ]
# historical_stocks.csv : fields = [ticker, exchange, name, sector, industry]
#                                   [0    , 1       , 2   , 3     , 4       ]

for line in sys.stdin:

    # split the line into fields
    fields = line.strip().split(',')

    try:
        if(len(fields) == 8):
            # condition verified if fields are from historical_stock_prices.csv file

            ticker,open,close,adj_close,low,high,volume,date = fields

            year = int(date[0:4])

            if((year >= 2008) and (year <= 2018)):

                print('{}\t{}\t{}\t{}\t-'.format(ticker,close,volume,date))

        else:
            # fields are from historical_stocks.csv file

            ticker,exchange,name,sector,industry = fields

            if(sector != 'N/A'):
                print('{}\t-\t-\t-\t{}'.format(ticker,sector))
    except: 
        # error line (maybe is csv header line)
        # skip line
        continue
    

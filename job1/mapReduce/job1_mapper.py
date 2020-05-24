#!/usr/bin/python3

import sys

# historical_stock_prices.csv : fields = [ticker, open, close, adj_close, low, high, volume, date]
#                                        [0     , 1   , 2    , 3        , 4  , 5   , 6     , 7   ]
# historical_stocks.csv : fields = "ticker, exchange, name, sector, industry"

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into fields
    fields = line.split(',')

    ticker,open,close,adj_close,low,high,volume,date = fields


    try:
        year = int(date[0:4])
        if((year >= 2008) and (year <= 2018)):
            # write the results to STDOUT (standard output);
            # what we output here will be the input for the
            # Reduce step, i.e. the input for statistics_reducer.py
            print('{}\t{}\t{}\t{}'.format(ticker,close,volume,date))
    except ValueError: 
        # year was not a number (maybe is csv header line)
        # skip line
        continue
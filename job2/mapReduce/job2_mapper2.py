#!/usr/bin/python3

import sys

for line in sys.stdin:

    # split the line into fields
    fields = line.strip().split('\t')

    sector,ticker,close,volume,date = fields
  
    print('{}\t{}\t{}\t{}\t{}'.format(sector,ticker,close,volume,date))
    

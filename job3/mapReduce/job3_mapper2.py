#!/usr/bin/python3

import sys


for line in sys.stdin:

    # split the line into fields
    fields = line.strip().split('\t')

    company,ticker,year,variation = fields

    print('{}\t{}\t{}\t{}'.format(company,ticker,year,variation))
    

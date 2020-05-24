#!/usr/bin/python3

import sys
from datetime import datetime as dt

current_sector = None
is_sector_line = False

for line in sys.stdin:

    # split the input line into fields
    fields = line.strip().split('\t')

    try :
        # parse the input from mapper1
        ticker,close,volume,date,sector = fields
        #ticker = fields[0]
        #sector = fields[4]
        
        # the first line should contain sector information
        if(sector != '-'):
            current_sector = sector
            is_sector_line = True
        else:
            is_sector_line = False
        
        if not is_sector_line:
            #close = float(fields[1])
            #volume = int(fields[2])
            #date = dt.strptime(fields[3],'%Y-%m-%d').date()
            #year = int(fields[3][0:4])
            print('{}\t{}\t{}\t{}\t{}'.format(current_sector,ticker,close,volume,date))
    except:
        # skip line
        continue



#!/usr/bin/python3

import sys
from datetime import datetime

trend_dict = {}
companies_dict = {}
current_company = None
is_company_line = False

for line in sys.stdin:

    # split the line into fields
    fields = line.strip().split('\t')

    try:

        #parse the input from mapper1
        ticker,close,date,company = fields

        # the first line should contain company information
        if(company != '-'):
            current_company = company
            is_company_line = True
        else:
            is_company_line = False
        
        if not is_company_line:

            #print('{}\t{}\t{}\t{}'.format(current_company,ticker,close,date))

            close = float(fields[1])
            date = datetime.strptime(fields[2],'%Y-%m-%d').date()
            year = int(fields[2][0:4])

            if(ticker not in companies_dict):
                companies_dict[ticker] = current_company

            if((ticker,year) not in trend_dict):
                trend_dict[(ticker,year)]=[date,close,date,close]
            else:
                if(date < trend_dict[(ticker,year)][0]):
                    trend_dict[(ticker,year)][0] = date
                    trend_dict[(ticker,year)][1] = close
                elif(date > trend_dict[(ticker,year)][2]):
                    trend_dict[(ticker,year)][2] = date
                    trend_dict[(ticker,year)][3] = close
    except:
        #skip line
        continue

        
for (ticker,year) in trend_dict.keys():

    x = float(trend_dict[(ticker,year)][1])
    y = float(trend_dict[(ticker,year)][3])

    variation = round((y/x)*100 - 100)

    comp = companies_dict[ticker]
    
    print('{}\t{}\t{}\t{}'.format(comp,ticker,year,variation))
#!/usr/bin/python3

import sys
from datetime import datetime as dt

trend_dict = {} # dict of ticker per year trend
sectors_dict = {} # dict to mantain ticker-sector link
final_dict = {} # dict of sector per year trend

for line in sys.stdin:

    # split the input line into fields
    fields = line.strip().split('\t')

    #sector,ticker,close,volume,date = fields
    sector = fields[0]
    ticker = fields[1]
    close = float(fields[2])
    volume = int(fields[3])
    date = dt.strptime(fields[4],'%Y-%m-%d').date()
    year = int(fields[4][0:4])

    # dict to mantain ticker-sector link
    if(ticker not in sectors_dict):
        sectors_dict[ticker] = sector

    #  insert new ticket-year data
    if((ticker,year) not in trend_dict):
        trend_dict[(ticker,year)]=[volume,close,1,date,close,date,close]
    else:
        # ticker-year already in dict
        # cumulate util values: volume, close, count
        trend_dict[(ticker,year)][0] += volume
        trend_dict[(ticker,year)][1] += close
        trend_dict[(ticker,year)][2] += 1
        # find ticker-year initial and final close value
        if(date < trend_dict[(ticker,year)][3]):
            trend_dict[(ticker,year)][3] = date
            trend_dict[(ticker,year)][4] = close
        elif(date > trend_dict[(ticker,year)][5]):
            trend_dict[(ticker,year)][5] = date
            trend_dict[(ticker,year)][6] = close

        
for (ticker,year) in trend_dict.keys():

    # calculate average values for ticker-year
    avg_volume = round((trend_dict[(ticker,year)][0]) / (trend_dict[(ticker,year)][2]))
    avg_close = (trend_dict[(ticker,year)][1]) / (trend_dict[(ticker,year)][2])

    final_close = float(trend_dict[(ticker,year)][6])
    initial_close = float(trend_dict[(ticker,year)][4])
    avg_variation = round((final_close/initial_close)*100 - 100)

    # get ticker's corresponding sector
    sector = sectors_dict[ticker]

    #  insert new sector-year data for every ticker avg values
    if((sector,year) not in final_dict):
        final_dict[(sector,year)]=[avg_volume,avg_variation,avg_close,1]
    else:
        # sector-year already in dict
        # cumulate ticker-year avg values
        final_dict[(sector,year)][0] += avg_volume
        final_dict[(sector,year)][1] += avg_variation
        final_dict[(sector,year)][2] += avg_close
        final_dict[(sector,year)][3] += 1

for (sector,year) in final_dict.keys():

    # calculate average values for sector-year
    final_volume = round((final_dict[(sector,year)][0]) / (final_dict[(sector,year)][3]))
    final_variation = round((final_dict[(sector,year)][1]) / (final_dict[(sector,year)][3]))
    final_close = round((final_dict[(sector,year)][2]) / (final_dict[(sector,year)][3]))

    print('{}\t{}\t{}\t{}%\t{}'.format(sector,year,final_volume,final_variation,final_close))


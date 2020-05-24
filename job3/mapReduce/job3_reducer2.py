#!/usr/bin/python3

import sys

avg_dict = {}
avg_list = []
companies_dict = {}
simil_trend_companies = {}
are_simil_trends = False

def compareTuples(list1,list2):
    try:
        x = abs(list1[0] - list2[0])
        y = abs(list1[1] - list2[1])
        z = abs(list1[2] - list2[2])
        if (x+y+z) <= 1:
            return True
        else:
            return False
    except:
        return False


for line in sys.stdin:

    # split the line into fields
    fields = line.strip().split('\t')

    try:
        #company,ticker,year,variation = fields
        company = fields[0]
        ticker = fields[1]
        year = int(fields[2])
        variation = float(fields[3])

        if((company,year) not in avg_dict):
            avg_dict[(company,year)] = [variation,1]
        
        # for companies that has more than 1 ticker, calculate the average variation for the company in that year
        else: 
            avg_dict[(company,year)][0] += variation
            avg_dict[(company,year)][1] += 1
    except:
        continue
    

for (company,year) in avg_dict.keys():
    avg = round(avg_dict[(company,year)][0] / avg_dict[(company,year)][1])

    avg_list.append([company,year,avg])

for row in avg_list:

    #company,year,variation = row
    company = row[0]
    year = int(row[1])
    variation = float(row[2])

    if(company not in companies_dict):
        companies_dict[company] = [None, None, None]

    if (year == 2016):
        companies_dict[company][0] = variation    
    elif (year == 2017):
        companies_dict[company][1] = variation
    else: # year == 2018
        companies_dict[company][2] = variation

for comp in companies_dict.keys():

    for name in companies_dict.keys():
        if(comp != name):
            are_simil_trends = compareTuples(companies_dict[comp],companies_dict[name])
            #if((companies_dict[comp] == companies_dict[name])):
            if(are_simil_trends):
                if(comp in simil_trend_companies):
                    simil_trend_companies[comp].append(name)
                elif(name in simil_trend_companies):
                    simil_trend_companies[name].append(comp)
                else:
                    simil_trend_companies[comp] = []
                    simil_trend_companies[comp].append(name)

for elem in simil_trend_companies:

    c = []
    c.append(elem)
    c.extend(set(simil_trend_companies[elem]))
    x = companies_dict[elem][0]
    y = companies_dict[elem][1]
    z = companies_dict[elem][2]

    print('{}:2016:{}%,2017:{}%,2018:{}%'.format(c,x,y,z))
    
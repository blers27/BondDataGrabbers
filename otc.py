"""
This python script looks for the latest release for bond transactions, finds the latest issues for certain bonds, and creates a spreadsheet for them.
"""

import pandas as pd
pd.options.mode.chained_assignment = None  # default='warn'
import datetime
from dateutil.relativedelta import relativedelta, MO, FR


def getURL():
    today = datetime.datetime.today()
    monday = today-relativedelta(weekday=MO(-1))

	#gets last friday datetime obj.
    global friday
    friday = today-relativedelta(weekday=FR(-1))

	#Source reports data with Monday's date, but we stored it as a Friday date.
    global url
    url = 'https://market.jsda.or.jp/en/statistics/bonds/prices/otc/files/{year}/ES{date}.xls'.format(year = monday.strftime('%Y'), date = monday.strftime('%y%m%d'))


def convertXLSX(name, regex):
	#read Excel file from url, headers are in rows 23-24, untouched
    df = pd.read_excel(url, header=[23,24], dtype=str)

    names = []
	
    for item in regex:
		#find 'Issues' and filter containing desired bond
        dfx = df[df['Issues', df['Issues'].columns[0]].str.contains(regex[item], na=False)]
		#sort by values by 'Code' descending. First row would be latest bond transaction.
        dfy = dfx.sort_values(by=('Code', df['Code'].columns[0]),ascending = False).iloc[0]
		#adds row number to list of rows we are collecting
        names.append(dfy.name)

	#create final df that contains just the bond transactions we want
    dfz = df.iloc[names ,:]

	#inserts date we store it as
    dfz.insert(0,'Date',friday.strftime('%d-%b-%y'))

	#gets rid of numbers at end of bond name
    dfz.replace({'(JGB)\d*\((.0)\)': r'\1 \2', '(Tokyo Metropolitan)\d*': r'\1'}, inplace=True, regex=True)

	#save as csv file
    dfz.to_csv(name,index=False)


if __name__ == '__main__':
    getURL()
	bonds = {
		'JGB10' : 'JGB\d*\(10\)',
		'JGB20' : 'JGB\d*\(20\)',
		'TokyoMetro' : 'Tokyo Metropolitan\d{3,4}'
		}
    convertXLSX('otc.csv', bonds)

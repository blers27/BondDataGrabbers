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

    global friday
    friday = today-relativedelta(weekday=FR(-1))

    global url
    url = 'https://market.jsda.or.jp/en/statistics/bonds/prices/otc/files/{year}/ES{date}.xls'.format(year = monday.strftime('%Y'), date = monday.strftime('%y%m%d'))


def convertXLSX(name, regex):
    df = pd.read_excel(url, header=[23,24], dtype=str)

    names = []

    for item in regex:
        dfx = df[df['Issues', df['Issues'].columns[0]].str.contains(regex[item], na=False)]
        dfy = dfx.sort_values(by=('Code', df['Code'].columns[0]),ascending = False).iloc[0]
        names.append(dfy.name)

    dfz = df.iloc[names ,:]

    dfz.insert(0,'Date',friday.strftime('%d-%b-%y'))

    dfz.replace({'(JGB)\d*\((.0)\)': r'\1 \2', '(Tokyo Metropolitan)\d*': r'\1'}, inplace=True, regex=True)

    dfz.to_csv(name,index=False)


if __name__ == '__main__':
    getURL()
	bonds = {
		'JGB10' : 'JGB\d*\(10\)',
		'JGB20' : 'JGB\d*\(20\)',
		'TokyoMetro' : 'Tokyo Metropolitan\d{3,4}'
		}
    convertXLSX('otc.csv', bonds)

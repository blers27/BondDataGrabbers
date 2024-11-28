import pandas as pd
import re
import os
from haverhelpers import chromeBrowser
from time import sleep

class LSAP:
#    def __init__(self):
    
    #downloads excel file from source
    def getOperations():
        driver = chromeBrowser(headless=True)
        driver.get('https://www.rbnz.govt.nz/statistics/d3')
        driver.find_element_by_link_text('Open markets operations - D3 (1995-current)').click()
        print('Downloading XLSX...')
        while not os.path.exists('hd3.xlsx'):
            sleep(1)
        driver.quit()
        print('Downloaded.')
            
    #reads Excel file in local directory
    def readXLSX(sheet_name):
        print('Reading XLSX...')
        df = pd.read_excel('hd3.xlsx', sheet_name, index_col=0, header=2, dtype='object')
        df = df.iloc[1:-1]
        df.index = pd.to_datetime(df.index) #sometimes source removes hyphens from dates and Pandas doesn't catch this
        return df  
    
    #finds columns to split i.e. columns with word 'range' in it
    def findCols(df):
        cols = df.columns.tolist()
        r = re.compile('.*Range|range.*')
        split_cols = list(filter(r.match, cols))
        
        return split_cols
    
    #formats and splits selected columns
    def cleanDF(df, range_col, sheet_name, csv_name): 
        #we want to split the ranges in the these columns so that we can get a min and max
        
        print(f'Getting minima and maxima for sheet {sheet_name}...')
        for column in range_col:
            
            if column in df.columns:
                L = []
                
                for element in df[column].astype(str):
                    try:
                        if element == '-':
                            L.append(['', ''])
                        elif re.search('(?<=\d)[ ]?-[ ]?', element) == None:
                            #print("Okay")
                            L.append([element, element])
                        else:
                            L.append(re.split('(?<=\d)[ ]?-[ ]?', element))
                    except:
                        pass
        
                df[f'{column}_min'] = [i[0] for i in L]
                df[f'{column}_max'] = [i[1] for i in L]
        
    
        df.to_csv(f'{csv_name}.csv', date_format='%Y-%m-%d')
        print(f'Saved {csv_name}.csv')   
    
    def calcTenor(df):
        cols = df.columns.to_list()
        
        r = re.compile('.*Maturity|maturity.*')
        maturity = list(filter(r.match, cols))
        
        if maturity:
            maturity = maturity[0]
            df[maturity] = df[maturity].astype('datetime64')
            
            df['Tenor'] = df[maturity] - df.index.normalize()
            
            df['Held Date'] = df.index
            cond = df['Tenor'].dt.days < 0
            df.loc[cond, ['Held Date', maturity]] = df.loc[cond, [maturity, 'Held Date']].values
            df.set_index('Held Date', inplace=True)
            
            df['Tenor'] = df[maturity] - df.index.normalize()
            
            df[maturity] = df[maturity].astype('datetime64').dt.strftime('%Y%m%d')
            cols = df.columns.to_list()
            
            
            r = re.compile('.*Securities|securities.*')
            securities_col = list(filter(r.match, cols))[0]
            cols.insert(0, cols.pop(cols.index(securities_col)))
            
            cols.insert(0, cols.pop(cols.index('Tenor')))
            
            return df[cols]
        else:
            return df
    
    #aggregates multiple operations occuring in same day to daily
    def aggDay(df, csv_name):
        #line below important for aggregating and counting - as 0
        df = df.replace('-','0')
        df = df.replace('m', '', regex=True).dropna()
        df[df.columns[2:5]] = df[df.columns[2:5]].astype('float64')
        df = df.resample('d').sum()
        df.to_csv(csv_name+'_sum.csv')
        print('Saved '+csv_name+'_sum.csv')    
  
    
if __name__ == '__main__':
    os.system('cls')
    wantedSheets = {'LSAP - NZGBs':'NZGB', 
                    'LSAP - LGFAs':'LGFA',
                    'Repo - OMO':'repo',
                    'Reverse Repo - OMO':'reverse'
                    }
    aggSheets = ['LSAP - NZGBs', 'LSAP - LGFAs']
    
    LSAP.getOperations()    
    
    sheet = 'Reverse Repo - OMO'
    
    for sheet in wantedSheets:
        df = LSAP.readXLSX(sheet)
        split_cols = LSAP.findCols(df)
        df = LSAP.calcTenor(df)
        LSAP.cleanDF(df, split_cols, sheet, wantedSheets[sheet])
        
        if sheet in aggSheets:
            LSAP.aggDay(df, wantedSheets[sheet])

'''
d1 = df.loc[:,['Tenor', 'Eligible Securities']].dropna()

d1.duplicated()
'''
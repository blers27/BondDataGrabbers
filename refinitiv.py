import requests, json
import datetime
import argparse
from time import sleep

def cmdParser():
    parser = argparse.ArgumentParser(prog='Refinitiv Downloader', description='Program to download Refinitiv extractions')
    
    parser.add_argument('InstrumentListName', type=str, help='Name of instrument list already defined on Refinitiv website')
    parser.add_argument('ReportTemplate', type=str, help='Name of report template already defined on Refinitiv website')
    parser.add_argument('Filename', type=str, help='Filename to save as. Don\'t forget .csv')
    
    args = parser.parse_args()
    
    instr = args.InstrumentListName
    report = args.ReportTemplate
    filename = args.Filename
    
    return instr, report, filename


class Refinitiv:
    
    def __init__(self, instr, report, filename):
        self.getToken()
        self.getListID(instr)
        self.getInstruments()
        self.getReportTemplate(report)
        self.getContentFields()
        self.startExtractionJob()
        self.getData(filename)
        
    def getToken(self):
        urlGetToken = 'https://selectapi.datascope.refinitiv.com/RestApi/v1/Authentication/RequestToken'
        
        header1 = {'Content-Type': 'application/json'}
        
        username = "USERNAME"
        passwd = "PASSWORD"
        tokenRequestBody = json.dumps({'Credentials':{'Password':passwd,'Username':username}})
        
        response = requests.post(urlGetToken, tokenRequestBody, headers=header1)
        
        self.myAuthToken = response.json()['value']
    
    def getListID(self, listname):
        urlGetList = f"https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/InstrumentListGetByName(ListName='{listname}')"
        
        self.header = {'Content-Type': 'application/json', 'Authorization': 'Token ' + self.myAuthToken, 'Prefer': 'respond-async'}
        resp = requests.get(urlGetList, headers=self.header)
        
        self.listID = resp.json()['ListId']
    
    def getInstruments(self):
        urlGetInstruments = f"https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/InstrumentLists('{self.listID}')/ThomsonReuters.Dss.Api.Extractions.InstrumentListGetAllInstruments"
        
        resp = requests.get(urlGetInstruments, headers=self.header)
        self.instruments = []
        for item in resp.json()['value']:
            new_dict = {}
            new_dict['Identifier'] = item['Identifier']
            new_dict['IdentifierType'] = item['IdentifierType']
            self.instruments.append(new_dict)
    
    def getReportTemplate(self, MyReportTemplate):
        #MyReportTemplate = 'History'
        url = f"https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ReportTemplateGetByName(Name='{MyReportTemplate}')"
        
        response = requests.get(url, headers=self.header)
        self.templateID = response.json()['ReportTemplateId']
    
    def getContentFields(self):
        url = f"https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ReportTemplates('{self.templateID}')/ThomsonReuters.Dss.Api.Extractions.ReportTemplateGetContentFields"

        response = requests.get(url, headers=self.header)
        
        self.fields = [item['FieldName'] for item in response.json()['value']]
        #self.fields.remove('Close Price')
    
    def startExtractionJob(self):
        url = 'https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/ExtractRaw'
                
        today = datetime.datetime.today().strftime('%Y-%m-%d') #+ 'T00:00:00.000Z'
        last_week = datetime.datetime.today() - datetime.timedelta(days=7)
        last_week = last_week.strftime('%Y-%m-%d') #+ 'T00:00:00.000Z'
        
        body = {
            "ExtractionRequest": {
                "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.PriceHistoryExtractionRequest",
                "ContentFieldNames": self.fields,
                "IdentifierList": {
                    "@odata.type": "#ThomsonReuters.Dss.Api.Extractions.ExtractionRequests.InstrumentIdentifierList",
                    "InstrumentIdentifiers": self.instruments,
                    "ValidationOptions": None,
                    "UseUserPreferencesForValidationOptions": False
                },
                "Condition": {
                    "QueryStartDate": last_week,
                    "QueryEndDate": today
                }
            }
        }
        
        jsonRequest = json.dumps(body)
        
        response = requests.post(url, jsonRequest, headers=self.header)
        
        count = 0
        while response.status_code != 200:
            sleep(5)
            count += 1
            if count > 5:
                raise Exception(f'Extraction failed. Error Code {response.status_code}. {response.text}')
                
        self.jobID = response.json()['JobId']
    
    def getData(self, filename):       
        getResultURL=str("https://hosted.datascopeapi.reuters.com/RestApi/v1/Extractions/RawExtractionResults(\'"+self.jobID+"\')/$value")
        #print("Retrieving result from "+getResultURL)
        resp=requests.get(getResultURL,headers=self.header)
    
        with open(f'{filename}', 'w', newline='') as f:
            f.write(resp.text)


if __name__ == '__main__':

    #'NorthAmericaBonds', 'History', 'test.csv'
    instr, report, filename = cmdParser()
    
    #instr, report, filename = 'Forward END_FWD fwd.csv'.split()
    #instr, report, filename = 'NorthAmericaBonds History nbonds.csv'.split()
    
    
    Refinitiv(instr, report, filename)

import pandas as pd
import abc
import requests

class FinancialDetailsInterface(metaclass=abc.ABCMeta):

    @abc.abstractclassmethod
    def __getDetails__():
        '''Get details of financial metadata'''
        raise NotImplementedError

    @classmethod
    def __subclasshook__(self, cls, C):
        if cls is FinancialInformationInterface:
             if any("__getDetails__" in B.__dict__ for B in C.__mro__):
                  return True
        return NotImplemented
    
class FinancialInformationInterface(metaclass=abc.ABCMeta):
     
    @abc.abstractclassmethod
    def __init__(self):
         pass
    
    @abc.abstractclassmethod
    def __extract_data__(self):
          pass
    
    @abc.abstractclassmethod
    def __consolidate_data__(self):
          pass

    @classmethod
    def __subclasshook__(cls, C):
        if cls is FinancialInformationInterface:
             if any("__extract_data__" in B.__dict__ for B in C.__mro__) and any("__consolidate_data__" in B.__dict__ for B in C.__mro__):
                  return True
        return NotImplemented
    

def export_to_excel(file_name, financial_statement_pd):
    financial_statement_pd.to_excel("/home/mateusz/Desktop/financials/stock-analysis/stocks/results/" + file_name + ".xlsx")

def convert_nasdaq_date_to_quarter(response_pd):
    response_pd["quarter"] = pd.to_datetime(response_pd.index)
    response_pd["quarter"] = response_pd["quarter"].dt.to_period('Q').dt.strftime('Q%q %Y')
    response_pd.index = response_pd["quarter"]
    response_pd.set_index('quarter')
    response_pd = response_pd.drop(columns="quarter")
    
    return response_pd

def prepare_nasdaq_response(request):
    response = requests.get(request)
    response_json = response.json()['dataset']
    response_data = response_json['data']
    response_columns = response_json['column_names']
    response_pd = pd.DataFrame(response_data,columns=response_columns).set_index('Date')
    response_pd = convert_nasdaq_date_to_quarter(response_pd)

    return response_pd

def prepare_bls_response(start_year, end_year):
    consolidated_data = []

    def request_run(request_start_year, request_end_year):
        get_request=(f'https://api.bls.gov/publicAPI/v2/timeseries/data/LNS14000000?registrationkey=192bc6fcfcc94a09a187a75157d8ee6c&startyear={request_start_year}&endyear={request_end_year}')
        response = requests.get(get_request)
        response_data = response.json()['Results']['series'][0]['data']
        return response_data
             
    def check_if_loop(request_start_year, request_end_year, consolidated_data):
        if int(end_year) - int(request_start_year) > 20:
            new_end_year = request_start_year + 20
            for record in request_run(request_start_year, request_end_year):
                consolidated_data.append(record)
            new_start_year = int(request_start_year) + 20
            new_end_year = int(new_end_year) + 20
            check_if_loop(new_start_year, new_end_year, consolidated_data)
        else:
            for record in request_run(request_start_year, request_end_year):
                consolidated_data.append(record)

        return consolidated_data
    
    def quarter_conversion(response_pd):
        response_pd["quarter"] = None
        for i, record in response_pd.iterrows():
            if record['period'] == "M03":
                record["quarter"] = "Q1 " + str(record['year'])
            elif record['period'] == "M06":
                record["quarter"] = "Q2 " + str(record['year'])
            elif record['period'] == "M09":
                record["quarter"] = "Q3 " + str(record['year'])
            elif record['period'] == "M12":
                record["quarter"] = "Q4 " + str(record['year'])

        for i, record in response_pd.iterrows():
            if record['period'] not in ["M03","M06","M09","M12"]:
                response_pd = response_pd.drop([i])


        return response_pd

    consolidated_data = check_if_loop(start_year, end_year, consolidated_data)

    response_pd = pd.DataFrame(consolidated_data).sort_values(by=['year','period'])
    response_pd = quarter_conversion(response_pd).drop(columns=['footnotes','latest','year','period','periodName'])
    response_pd.index = response_pd["quarter"]
    response_pd.set_index('quarter')
    response_pd = response_pd.drop(columns="quarter")

    return response_pd

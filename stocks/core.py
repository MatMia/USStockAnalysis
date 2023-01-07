import pandas as pd
import numpy as np
from datetime import date, timedelta
import requests

from dotenv import load_dotenv
load_dotenv()
import os

API_KEY = os.getenv("STOCK_API_KEY")

stocksTicker = 'AAPL'
multiplier = 1
timespan = 'day'
date_from = date.today() - timedelta(days=20)
date_to = date.today()


class StockPriceDetails():

    def get_stock_price_details():

        get_request = f"https://api.polygon.io/v2/aggs/ticker/{stocksTicker}/range/{multiplier}/{timespan}/{date_from}/{date_to}?apiKey={API_KEY}"

        response = requests.get(get_request)
        response_results = pd.DataFrame(response.json()['results'])
        response_ticker = response.json()['ticker']

        return(response_results)

class StockNews():
    def get_stock_news():

        newsTimestamp = date.today() - timedelta(days=10)
        get_request = f"https://api.polygon.io/v2/reference/news?ticker={stocksTicker}&published_utc.gte={newsTimestamp}&apiKey={API_KEY}"

        response = requests.get(get_request)
        response_results = pd.DataFrame(response.json()['results'])

        return(response_results)

class StockFinancials:
    def __init__(self):
        self.stocksTicker = stocksTicker

    def breakdown_financial_statement(self, financial_category_statement):
        financial_category_pd = pd.DataFrame()
        for key in financial_category_statement:
            financial_category_pd = financial_category_pd.append(pd.DataFrame.from_dict([financial_category_statement[key]]))

        if financial_category_pd.empty:
            return None
        else:
            financial_category_pd["value"] = financial_category_pd["value"].round(4)
            print(financial_category_pd)
            return financial_category_pd.reset_index(drop=True)
        

    def get_stock_financials(self):
        get_request = f"https://api.polygon.io/vX/reference/financials?ticker={self.stocksTicker}&filing_date.gte=2009-01-01&limit=1&apiKey={API_KEY}"

        response = requests.get(get_request)
        response_results = response.json()['results']

        for record in response_results:
            # print(response_results)
            for key in record['financials']:
                if key == 'income_statement' : income_statement = self.breakdown_financial_statement(record['financials']['income_statement'])
                if key == 'comprehensive_income' : comprehensive_income = self.breakdown_financial_statement(record['financials']['comprehensive_income'])
                if key == 'balance_sheet' : balance_sheet = self.breakdown_financial_statement(record['financials']['balance_sheet'])
                if key == 'cash_flow_statement' : cash_flow_statement = self.breakdown_financial_statement(record['financials']['cash_flow_statement'])

        return(response_results)



# print(StockPriceDetails.get_stock_price_details())
# print(StockNews.get_stock_news())
stock_financials = StockFinancials()
stock_financials.get_stock_financials()

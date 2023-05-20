import pandas as pd
pd.options.display.float_format = '{:,.2f}'.format
from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
import requests

from dotenv import load_dotenv
load_dotenv()
import os

from helpers import export_to_excel
from helpers import FinancialDetailsInterface, FinancialInformationInterface
from helpers import prepare_nasdaq_response, prepare_bls_response

POLYGON_API_KEY = os.getenv("POLYGON_STOCK_API_KEY")
NASDAQ_API_KEY = os.getenv("NASDAQ_STOCK_API_KEY")

# *** Utils

class StockPriceDetails(FinancialDetailsInterface):
    def __init__(self, stockTicker, multiplier, timespan, date_from, date_to):
        self.stockTicker = stockTicker
        self.multiplier = multiplier
        self.timespan = timespan
        self.date_from = date_from
        self.date_to = date_to

    def __getDetails__(self):

        #Price is passed with first day of a new quarter. As a result, price range is for previous quarter period, thus the translation difference.
        quarters_translation = {
            1 : 'Q4',
            2 : 'Q1',
            3 : 'Q2',
            4 : 'Q3'
        }

        def _verifyPriceYear(quarter, year):
            year_ago = year - relativedelta(years=1)
            result = [year_ago if quarter == 'Q4' else year]
            return  str(quarter) + ' ' + str(datetime.strftime(result[0], ("%Y")))

        def _getRequest():
            get_request = f"https://api.polygon.io/v2/aggs/ticker/{self.stockTicker}/range/{self.multiplier}/{self.timespan}/{self.date_from}/{self.date_to}?limit=50000&apiKey={POLYGON_API_KEY}"
            return get_request

        response = requests.get(_getRequest())
        response_results_pd = pd.DataFrame(response.json()['results'])
        response_results_pd = response_results_pd.rename(columns={'v':'Volume', 'vw':'Average_price','o':'Open_price','c':'Close_price','h':'High_price','l':'Low_price','t':'Time','n':'Number_of_transactions'})
        response_results_pd['Volume'] = response_results_pd['Volume'].astype(int)
        response_results_pd["Time"] = pd.to_datetime(response_results_pd["Time"], unit='ms')
        response_results_pd["quarter"] = response_results_pd["Time"].dt.quarter.apply(quarters_translation.get)
        response_results_pd["year"] = response_results_pd["Time"].dt.strftime("%Y")
        response_results_pd["period"] = response_results_pd.apply(lambda x: _verifyPriceYear(x["quarter"], x["Time"]), axis=1)
        response_results_pd["AvgTransactionVolume"] = response_results_pd["Volume"] / response_results_pd["Number_of_transactions"]
        response_results_pd = response_results_pd.drop(columns=['quarter','year', 'Time'])
        response_stock_ticker = response.json()['ticker']
        assert self.stockTicker == response_stock_ticker

        return(response_results_pd)

class StockNews():
    def get_stock_news(stocksTicker):

        newsTimestamp = date.today() - timedelta(days=10)
        get_request = f"https://api.polygon.io/v2/reference/news?ticker={stocksTicker}&published_utc.gte={newsTimestamp}&apiKey={POLYGON_API_KEY}"

        response = requests.get(get_request)
        response_results = pd.DataFrame(response.json()['results'])

        return(response_results)

class StockFinancials(FinancialDetailsInterface):
    def __init__(self, stockTicker):
        self.stockTicker = stockTicker

    def breakdown_financial_statement(self, financial_category_statement, financial_statement_period):
        financial_category_pd = pd.DataFrame()
        for key in financial_category_statement:
            financial_category_pd = pd.concat([financial_category_pd, pd.DataFrame.from_dict([financial_category_statement[key]])])

        if financial_category_pd.empty:
            return None
        else:
            financial_category_pd["value"] = financial_category_pd["value"].astype(float)
            financial_category_pd = financial_category_pd.drop(columns=['order', 'unit']).rename({'value': financial_statement_period}, axis='columns')
            return financial_category_pd.reset_index(drop=True)

    def merge_financials_data(self, index, start_pd, new_pd):
        if (index == 0 or start_pd is None):
            returned_pd = new_pd
        elif new_pd is None:
            returned_pd = start_pd         
        else: 
            returned_pd = start_pd.merge(new_pd, left_on='label', right_on='label')
        return returned_pd

    def add_q4_data(self, returned_pd):
        returned_pd_with_q4 = returned_pd
        for column in returned_pd.columns:
            column_year = column[-4:]
            if column[:2] == 'FY':
                if self.financial_statement_type == "income_statement":
                    available_quarters = [x[:2] for x in returned_pd.columns if (x[-4:] == column_year and isinstance(x[:2], int))]
                    returned_pd_with_q4 = returned_pd_with_q4.rename(columns={('FY ' + str(column_year)): ('Q4 ' + str(column_year))})
                    for available_quarter in available_quarters:
                        returned_pd_with_q4['Q4 ' + str(column_year)] = returned_pd_with_q4['Q4 ' + str(column_year)] - returned_pd_with_q4[str(available_quarter) + ' ' + str(column_year)]
                else:
                    returned_pd_with_q4 = returned_pd_with_q4.rename(columns={('FY ' + str(column_year)): ('Q4 ' + str(column_year))})
        return returned_pd_with_q4

    def __getDetails__(self, financial_statement_type):

        self.financial_statement_type = financial_statement_type

        def get_stock_financials(self):
            """Allows to retrieve readable format of one of four financial statements:
            - income_statement
            - comprehensive_income
            - balance_sheet
            - cash_flow_statement
            \n Possible arguments: income_statement, comprehensive_income, balance_sheet, cash_flow_statement
            """

            get_request = f"https://api.polygon.io/vX/reference/financials?ticker={self.stockTicker}&filing_date.gte=2009-01-01&limit=100&apiKey={POLYGON_API_KEY}"

            readable_income_statement_pd = pd.DataFrame()
            readable_comprehensive_income_pd = pd.DataFrame()
            readable_balance_sheet_pd = pd.DataFrame()
            readable_cash_flow_statement_pd = pd.DataFrame()

            response = requests.get(get_request)
            response_results = response.json()['results']

            for i, record in enumerate(response_results):
                fiscal_period = record['fiscal_period']
                fiscal_year = record['fiscal_year']
                financial_statement_period = str(fiscal_period) + ' ' + str(fiscal_year)

                for key in record['financials']:
                    if key == 'income_statement':
                        readable_income_statement = self.breakdown_financial_statement(record['financials']['income_statement'], financial_statement_period)
                        readable_income_statement_pd = self.merge_financials_data(i, readable_income_statement_pd, readable_income_statement)
                    if key == 'comprehensive_income':
                        readable_comprehensive_income = self.breakdown_financial_statement(record['financials']['comprehensive_income'], financial_statement_period)
                        readable_comprehensive_income_pd = self.merge_financials_data(i, readable_comprehensive_income_pd, readable_comprehensive_income)
                    if key == 'balance_sheet':
                        readable_balance_sheet = self.breakdown_financial_statement(record['financials']['balance_sheet'], financial_statement_period)
                        readable_balance_sheet_pd = self.merge_financials_data(i, readable_balance_sheet_pd, readable_balance_sheet)
                    if key == 'cash_flow_statement':
                        readable_cash_flow_statement = self.breakdown_financial_statement(record['financials']['cash_flow_statement'], financial_statement_period)
                        readable_cash_flow_statement_pd = self.merge_financials_data(i, readable_cash_flow_statement_pd, readable_cash_flow_statement)

            returned_financial_statement = locals()['readable_' + self.financial_statement_type + '_pd']
            returned_financial_statement = self.add_q4_data(returned_financial_statement) 
            
            return(returned_financial_statement.reset_index())

        return get_stock_financials(self)

class TreasuryYieldCurveRates(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getYieldCurveRate():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/USTREASURY/YIELD.json?api_key={NASDAQ_API_KEY}&start_date=1950-01-01&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = _addInversionCurve2vs10(response_pd)
            return(response_pd)

        def _addInversionCurve2vs10(response_pd):
            response_pd['2vs10Y'] = response_pd['10 YR'] - response_pd['2 YR']
            return response_pd

        return _getYieldCurveRate()
    
class ConsumerSentiment(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getMichigenConsumerSentiment():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/UMICH/SOC1.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Index": "MichigenSentiment"})
            return(response_pd)
        
        return _getMichigenConsumerSentiment()
        
class SnP500_PE(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getSnP500_TrailingPriceToEarnings():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_PE_RATIO_MONTH.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Value": "SnP500_PE"})
            return(response_pd)

        return _getSnP500_TrailingPriceToEarnings()
    
class SnP500_Yield(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getSnP500_EarningsYield():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_EARNINGS_YIELD_MONTH.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Value": "SnP500_Yield"})
            return(response_pd)

        return _getSnP500_EarningsYield()

class SnP500_RealValue(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getSnP500_RealValue():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_REAL_PRICE_MONTH.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Value": "SnPIndexValue"})
            return(response_pd)

        return _getSnP500_RealValue()
    
class SnP500_EarningsYoYGrowth(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getSnP500_EarningsYoYGrowth():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_EARNINGS_GROWTH_QUARTER.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Value": "SnPEarningsYoYGrowth"})
            return(response_pd)

        return _getSnP500_EarningsYoYGrowth()
    
class SnP500_Earnings(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getSnP500_Earnings():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/MULTPL/SP500_EARNINGS_MONTH.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Value": "SnPEarnings"})
            return(response_pd)

        return _getSnP500_Earnings()
    
class USInflation(FinancialDetailsInterface):
    
    def __getDetails__():

        def _getUSInflation():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/RATEINF/INFLATION_USA.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Value": "US Inflation"})
            return(response_pd)

        return _getUSInflation()

class USUnemployment(FinancialDetailsInterface):
    
    def __getDetails__(source):
        """
        Extracts US Enemployment data.
        \n Possible arguments (data source):
        - NASDAQ
        - BLS
        """
        def _getUSUnemployment_NASDAQ():
            get_request = f"https://data.nasdaq.com/api/v3/datasets/FRED/UNRATE.json?api_key={NASDAQ_API_KEY}&collapse=quarterly"
            response_pd = prepare_nasdaq_response(get_request)
            response_pd = response_pd.rename(columns={"Value": "US Unemployment"})
            return(response_pd)
        
        def _getUSUnemployment_BLS():
            start_year = 1948
            # BLS Unemployment data is available since 1948 year only - https://beta.bls.gov/dataViewer/view/timeseries/LNS14000000
            end_year = datetime.now().strftime('%Y')
            response_pd = prepare_bls_response(start_year, end_year)
            response_pd = response_pd.rename(columns={"value": "US Unemployment"})
            response_pd = response_pd.astype({'US Unemployment' : float})
            return(response_pd)

        if source == "NASDAQ":
            return _getUSUnemployment_NASDAQ()
        elif source == "BLS":
            return _getUSUnemployment_BLS()
        else:
            return KeyError(TypeError)

class CompanyFinancialInformation(FinancialInformationInterface):

    def __init__(self, stockTicker, multiplier, timespan, date_from, date_to):
        self.stockTicker = stockTicker
        self.multiplier = multiplier
        self.timespan = timespan
        self.date_from = date_from
        self.date_to = date_to

    def __extract_data__(self):

        # Stock Price
        stock_prices = StockPriceDetails(self.stockTicker, self.multiplier, self.timespan, self.date_from, self.date_to)
        stock_prices_details = stock_prices.__getDetails__()
        transposed_stock_prices = stock_prices_details.transpose()
        transposed_stock_prices.columns=stock_prices_details["period"]
        self.transposed_stock_prices = transposed_stock_prices.drop("period")

        # Stock Financial
        stock_financials = StockFinancials(self.stockTicker)
        #income_statement
        self.received_income_statement = stock_financials.__getDetails__('income_statement').set_index("label")
        #balance_sheet
        self.balance_sheet_statement = stock_financials.__getDetails__("balance_sheet").set_index("label")
        
        # Yield Curve Rates
        interest_rates = TreasuryYieldCurveRates
        self.interest_rates_details = interest_rates.__getDetails__()

        #Consumer Sentiment (University Michigen)
        consumer_sentiment = ConsumerSentiment
        self.consumer_sentiment_details = consumer_sentiment.__getDetails__()

        #SnP500 Price to Earnings
        snp500_pe = SnP500_PE
        self.snp500_pe_details = snp500_pe.__getDetails__()

        #SnP500 Earnings Yield
        snp500_yield = SnP500_Yield
        self.snp500_yield_details = snp500_yield.__getDetails__()

        #SnP500 Index Value
        snp500_indexValue = SnP500_RealValue
        self.snp500_indexValue_details = snp500_indexValue.__getDetails__()

        #US Inflation
        us_inflation = USInflation
        self.us_inflation = us_inflation.__getDetails__()

        #US Unemployment
        us_undemployment = USUnemployment
        self.us_unemployment = us_undemployment.__getDetails__("BLS")

    def __consolidate_data__(self):

        self.__extract_data__()
        # Consolidate stock prices, financial statements, yield curve rates data and consumer sentiment
        merged_data = pd.concat([self.received_income_statement, self.transposed_stock_prices],axis=0,join="inner")
        merged_data = pd.concat([merged_data, self.balance_sheet_statement],axis=0,join="inner")
        merged_data = pd.concat([merged_data, self.interest_rates_details.transpose()],axis=0, join="inner")
        merged_data = pd.concat([merged_data, self.consumer_sentiment_details.transpose()],axis=0, join="inner")
        merged_data = pd.concat([merged_data, self.snp500_pe_details.transpose()],axis=0, join="inner")
        merged_data = pd.concat([merged_data, self.snp500_yield_details.transpose()],axis=0, join="inner")
        merged_data = pd.concat([merged_data, self.snp500_indexValue_details.transpose()],axis=0, join="inner")
        merged_data = pd.concat([merged_data, self.us_inflation.transpose()],axis=0, join="inner")
        merged_data = pd.concat([merged_data, self.us_unemployment.transpose()],axis=0,join='inner')

        return merged_data

class GeneralFinancialInformation(FinancialInformationInterface):

    def __init__(self):
        pass

    def __extract_data__(self):

        # Yield Curve Rates
        interest_rates = TreasuryYieldCurveRates
        self.interest_rates_details = interest_rates.__getDetails__()

        #Consumer Sentiment (University Michigen)
        consumer_sentiment = ConsumerSentiment
        self.consumer_sentiment_details = consumer_sentiment.__getDetails__()

        #SnP500 Price to Earnings
        snp500_pe = SnP500_PE
        self.snp500_pe_details = snp500_pe.__getDetails__()

        #SnP500 Earnings Yield
        snp500_yield = SnP500_Yield
        self.snp500_yield_details = snp500_yield.__getDetails__()

        #SnP500 Index Value
        snp500_indexValue = SnP500_RealValue
        self.snp500_indexValue_details = snp500_indexValue.__getDetails__()

        #SnP500 Earnings
        snp500_earnings = SnP500_Earnings
        self.snp500_earnings_details = snp500_earnings.__getDetails__()

        #SnP500 Earnings Growth Year On Year
        snp500_earningsYoY = SnP500_EarningsYoYGrowth
        self.snp500_earningsYoY_details = snp500_earningsYoY.__getDetails__()

        #US Inflation
        us_inflation = USInflation
        self.us_inflation = us_inflation.__getDetails__()

        #US Unemployment
        us_undemployment = USUnemployment
        self.us_unemployment = us_undemployment.__getDetails__("BLS")

    def __consolidate_data__(self):
            
        self.__extract_data__()
        # Consolidate yield curve rates data and consumer sentiment
        merged_data = pd.concat([self.interest_rates_details.transpose(), self.consumer_sentiment_details.transpose()],axis=0, join="outer")
        merged_data = pd.concat([merged_data, self.snp500_pe_details.transpose()],axis=0, join="outer")
        merged_data = pd.concat([merged_data, self.snp500_yield_details.transpose()],axis=0, join="outer")
        merged_data = pd.concat([merged_data, self.snp500_indexValue_details.transpose()],axis=0, join="outer")
        merged_data = pd.concat([merged_data, self.snp500_earnings_details.transpose()],axis=0, join="outer")
        merged_data = pd.concat([merged_data, self.snp500_earningsYoY_details.transpose()],axis=0, join="outer")
        merged_data = pd.concat([merged_data, self.us_inflation.transpose()],axis=0, join="outer")
        merged_data = pd.concat([merged_data, self.us_unemployment.transpose()],axis=0,join='outer')
        return merged_data


# *** Runner

# uber_financial_information = CompanyFinancialInformation('UBER', 1, 'quarter', date.today() - timedelta(days=5825), date.today())
# uber_financial_information_consolidated = uber_financial_information.__consolidate_data__()
# export_to_excel("financial_info", uber_financial_information_consolidated)

general_information = GeneralFinancialInformation()
general_financial_info = general_information.__consolidate_data__()
export_to_excel("general_financial_info", general_financial_info)

# print(SnP500_RealValue.__getDetails__())
import pandas as pd
import fundamentalanalysis as fa
from smart_value.stock import *

"""
Data provided by Financial Modeling Prep
https://site.financialmodelingprep.com/developer/docs/
Python API wrapper by
https://github.com/JerBouma/FundamentalAnalysis
"""


class FmpData(Stock):
    """Retrieves the data from FMP API"""

    def __init__(self, symbol):
        """
        :param symbol: string ticker of the stock
        """
        super().__init__(symbol)
        self.load_attributes()

    def load_attributes(self):
        api_key = 'c99eda5db224d34162adae341298790b'
        profile = fa.profile(self.symbol, api_key)
        quote = fa.quote(self.symbol, api_key)

        self.name = profile.loc['companyName']
        self.sector = profile.loc['companyName']
        self.price = [profile.loc['price'], profile.loc['currency']]
        self.exchange = profile.loc['exchangeShortName']
        self.shares = quote.loc['sharesOutstanding']
        self.annual_bs = fa.balance_sheet_statement(self.symbol, api_key, period="annual")
        self.quarter_bs = fa.balance_sheet_statement(self.symbol, api_key, period="quarterly")
        self.is_df = fa.income_statement(self.symbol, api_key, period="annual")
        self.cf_df = fa.cash_flow_statement_annually = fa.cash_flow_statement(self.symbol, api_key, period="annual")
        self.report_currency = self.is_df.iloc[:, :1].loc['reportedCurrency']
        try:
            self.last_dividend = -int(self.cf_df.loc['CashDividendsPaid'][0]) / self.shares
        except ZeroDivisionError:
            self.last_dividend = 0
        try:
            self.buyback = -int(self.cf_df.loc['RepurchaseOfCapitalStock'][0]) / self.shares
        except ZeroDivisionError:
            self.buyback = 0
        # left most column contains the most recent data
        self.last_fy = self.annual_bs.columns[0]
        self.most_recent_quarter = self.quarter_bs.columns[0]

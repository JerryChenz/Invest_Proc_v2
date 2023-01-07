import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime
from yfinance import Ticker


class Financials:
    """Retrieves the data from YH Finance API and yfinance package"""

    def __init__(self, ticker):
        self.ticker = ticker
        # yfinance
        try:
            self.stock_data = Ticker(self.ticker)
        except KeyError:
            print("Check your stock ticker")
        self.name = self.stock_data.info['shortName']
        self.price = [self.stock_data.info['currentPrice'], self.stock_data.info['currency']]
        self.exchange = self.stock_data.info['exchange']
        self.shares = self.stock_data.info['sharesOutstanding']
        self.report_currency = self.stock_data.info['financialCurrency']
        self.next_earnings = pd.to_datetime(datetime.fromtimestamp(self.stock_data.info['mostRecentQuarter'])
                                            .strftime("%Y-%m-%d")) + pd.DateOffset(months=6)
        self.annual_bs = self.get_balance_sheet("annual")
        self.quarter_bs = self.get_balance_sheet("quarter")
        self.income_statement = self.get_income_statement()
        self.avg_gross_margin = self.income_statement['Gross_margin'].mean(axis=1)
        print(self.avg_gross_margin)
        self.geo_sales_growth = stats.gmean(self.income_statement.loc['TotalRevenue'], axis=1)
        self.avg_ebit_margin = self.income_statement['Ebit_margin'].mean(axis=1)
        self.geo_ebit_growth = stats.gmean(self.income_statement.loc['Ebit'], axis=1)
        self.avg_net_margin = self.income_statement['Net_margin'].mean(axis=1)
        self.geo_ni_growth = self.income_statement['NetIncomeCommonStockholders'].mean(axis=1)
        self.cash_flow = self.get_cash_flow()
        try:
            self.dividends = -int(self.cash_flow.loc['CommonStockDividendPaid'][0]) / self.shares
        except ZeroDivisionError:
            self.dividends = 0
        try:
            self.buyback = -int(self.cash_flow.loc['RepurchaseOfCapitalStock'][0]) / self.shares
        except ZeroDivisionError:
            self.buyback = 0

    def get_balance_sheet(self, option):
        """Returns a DataFrame with selected balance sheet data"""

        dummy = {
            "Dummy": [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]}

        if option == "annual":
            balance_sheet = self.stock_data.get_balance_sheet()
            bs_index = ['TotalAssets', 'CurrentAssets', 'CurrentLiabilities', 'CurrentDebtAndCapitalLeaseObligation',
                        'CurrentCapitalLeaseObligation', 'LongTermDebtAndCapitalLeaseObligation',
                        'LongTermCapitalLeaseObligation', 'TotalEquityGrossMinorityInterest', 'MinorityInterest',
                        'CashAndCashEquivalents', 'OtherShortTermInvestments', 'InvestmentProperties',
                        'LongTermEquityInvestment', 'InvestmentinFinancialAssets', 'NetPPE']
        else:
            balance_sheet = self.stock_data.quarterly_balance_sheet
            # Quarter balance sheet index is different from annual bs
            bs_index = ['Total Assets', 'Current Assets', 'Current Liabilities',
                      'Current Debt And Capital Lease Obligation',
                      'Current Capital Lease Obligation',
                      'Long Term Debt And Capital Lease Obligation',
                      'Long Term Capital Lease Obligation',
                      'Total Equity Gross Minority Interest',
                      'MinorityInterest', 'Cash And Cash Equivalents',
                      'Other Short Term Investments', 'Investment Properties',
                      'Long Term Equity Investment', 'Investmentin Financial Assets', 'Net PPE']
        # Start of Cleaning: make sure the data has all the required indexes
        dummy_df = pd.DataFrame(dummy, index=bs_index)
        clean_bs = dummy_df.join(balance_sheet)
        bs_df = clean_bs.loc[bs_index]
        # Ending of Cleaning: drop the dummy column after join
        bs_df.drop('Dummy', inplace=True, axis=1)

        return bs_df.fillna(0)

    def get_income_statement(self):
        """Returns a DataFrame with selected income statement data"""

        income_statement = self.stock_data.get_income_stmt()
        # Start of Cleaning: make sure the data has all the required indexes
        dummy = {"Dummy": [None, None, None, None, None]}
        is_index = ['TotalRevenue', 'CostOfRevenue', 'SellingGeneralAndAdministration', 'InterestExpense',
                    'NetIncomeCommonStockholders']
        dummy_df = pd.DataFrame(dummy, index=is_index)
        clean_is = dummy_df.join(income_statement)
        is_df = clean_is.loc[is_index]
        try:
            is_df['Gross_margin'] = np.round(is_df['CostOfRevenue'] / is_df['TotalRevenue'] * 100, 2)
            print(is_df)
        except ZeroDivisionError:
            is_df['Gross_margin'] = 0
        try:
            is_df['Ebit'] = np.round(is_df['TotalRevenue'] - is_df['CostOfRevenue']
                                     - is_df['SellingGeneralAndAdministration'] * 100, 2)
        except ZeroDivisionError:
            is_df['Ebit'] = 0
        try:
            is_df['Ebit_margin'] = np.round(is_df['Ebit'] / is_df['TotalRevenue'] * 100, 2)
        except ZeroDivisionError:
            is_df['Ebit_margin'] = 0
        try:
            is_df['Net_margin'] = np.round(is_df['NetIncomeCommonStockholders'] / is_df['TotalRevenue'] * 100, 2)
        except ZeroDivisionError:
            is_df['Net_margin'] = 0
        # Ending of Cleaning: drop the dummy column after join
        is_df.drop('Dummy', inplace=True, axis=1)

        return is_df.fillna(0)

    def get_cash_flow(self):
        """Returns a DataFrame with selected Cash flow statement data"""

        cash_flow = self.stock_data.get_cashflow()
        # Start of Cleaning: make sure the data has all the required indexes
        dummy = {"Dummy": [None]}
        cf_index = ['CommonStockDividendPaid', 'RepurchaseOfCapitalStock']
        dummy_df = pd.DataFrame(dummy, index=cf_index)
        clean_cf = dummy_df.join(cash_flow)
        cf_df = clean_cf.loc[cf_index]
        # Ending of Cleaning: drop the dummy column after join
        cf_df.drop('Dummy', inplace=True, axis=1)

        return cf_df.fillna(0)
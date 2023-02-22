from smart_value.asset import *
from smart_value.financial_data import yf_data as yf
from smart_value.financial_data import yq_data as yq
import pandas as pd

# The stock summary should follow this format
standardized_summary = []


class Stock(Asset):
    """a type of Securities"""

    def __init__(self, asset_code, source):
        """
        :param asset_code: string ticker of the stock
        :param source: data source selector
        """
        super().__init__(asset_code)

        self.sector = None
        self.report_currency = None
        self.annual_bs = None  # annual balance sheet data
        self.quarter_bs = None  # last quarter balance sheet data
        self.cf_df = None
        self.is_df = None
        # self.avg_gross_margin = None
        # self.avg_sales_growth = None
        # self.avg_ebit_margin = None
        # self.avg_ebit_growth = None
        # self.avg_net_margin = None
        # self.avg_ni_growth = None
        # self.years_of_data = None
        self.fx_rate = None
        self.buyback = None
        self.source = source
        self.load_data()

    def load_data(self):
        """data source selector."""

        if self.source == "yf":
            ticker_data = yf.Financials(self.asset_code)
            self.load_yahoo(ticker_data)
        elif self.source == "yf_quote":
            self.load_yf_quote()
        elif self.source == "yq":
            ticker_data = yq.YqData(self.asset_code)
            self.load_yahoo(ticker_data)
        elif self.source == "fmp":
            self.load_from_fmp()
        else:
            pass  # Other sources of data to-be-implemented

    def load_yahoo(self, ticker_data):
        """Scrap the financial_data from yfinance API

        :param ticker_data: financial data object
        """

        self.name = ticker_data.name
        self.sector = ticker_data.sector
        self.price = ticker_data.price
        self.exchange = ticker_data.exchange
        self.shares = ticker_data.shares
        self.report_currency = ticker_data.report_currency
        self.fx_rate = yf.get_forex(self.report_currency, self.price[1])
        self.periodic_payment = ticker_data.dividends
        self.buyback = ticker_data.buyback
        self.annual_bs = ticker_data.annual_bs
        self.quarter_bs = ticker_data.quarter_bs
        self.cf_df = ticker_data.cash_flow
        self.is_df = ticker_data.income_statement
        self.last_fy = ticker_data.annual_bs.columns[0]
        self.last_result = ticker_data.most_recent_quarter

    def load_yf_quote(self):
        """Scrap the financial_data from yfinance API"""

        market_price = yf.get_quote(self.asset_code, "last_price")
        price_currency = yf.get_quote(self.asset_code, "currency")
        report_currency = yf.get_info(self.asset_code, "financialCurrency")

        self.price = [market_price, price_currency]
        self.fx_rate = yf.get_forex(report_currency, price_currency)

    def load_from_fmp(self):
        """Scrap the financial_data from FMP API"""

        pass

    def current_summary(self):
        """Return a summary of all the key stock data.

        :return: All key stock data in one DataFrame
        """

        # balance sheet and income statement information
        current_bs = self.quarter_bs.iloc[:, :1]
        # standardize the column name to enable concat
        current_bs.columns = [self.last_fy]
        current_is = self.is_df.iloc[:, :1]
        # concat 2 Series column-wise
        stock_summary = pd.concat([current_bs, current_is]).T
        # transpose the DataFrame to one row
        stock_summary.transpose()
        # standardize the column names to prevent KeyError due to different data sources
        stock_summary.columns = ['TotalAssets', 'CurrentAssets', 'CurrentLiabilities',
                                 'CurrentDebtAndCapitalLeaseObligation', 'CurrentCapitalLeaseObligation',
                                 'LongTermDebtAndCapitalLeaseObligation', 'LongTermCapitalLeaseObligation',
                                 'TotalEquityGrossMinorityInterest', 'MinorityInterest', 'CashAndCashEquivalents',
                                 'OtherShortTermInvestments', 'InvestmentProperties', 'LongTermEquityInvestment',
                                 'InvestmentinFinancialAssets', 'NetPPE', 'TotalRevenue', 'CostOfRevenue',
                                 'SellingGeneralAndAdministration', 'InterestExpense', 'NetIncomeCommonStockholders',
                                 'GrossMargin', 'EBIT', 'EbitMargin', 'NetMargin']
        # ticker and dividend
        stock_summary.insert(loc=0, column='Ticker', value=self.asset_code)
        stock_summary.insert(loc=1, column='Name', value=self.name)
        stock_summary.insert(loc=2, column='Sector', value=self.sector)
        stock_summary.insert(loc=3, column='Exchange', value=self.exchange)
        stock_summary.insert(loc=4, column='Price', value=self.price[0])
        stock_summary.insert(loc=5, column='Price_currency', value=self.price[1])
        stock_summary.insert(loc=6, column='Shares', value=self.shares)
        stock_summary.insert(loc=7, column='Reporting_Currency', value=self.report_currency)
        stock_summary.insert(loc=8, column='Fx_rate', value=self.fx_rate)
        stock_summary.insert(loc=9, column='Dividend', value=self.periodic_payment)
        stock_summary.insert(loc=10, column='Buyback', value=self.buyback)
        stock_summary.insert(loc=11, column='Last_fy', value=self.last_fy)
        stock_summary['CFO'] = self.cf_df.loc['OperatingCashFlow']
        stock_summary['CFI'] = self.cf_df.loc['InvestingCashFlow']
        stock_summary['CFF'] = self.cf_df.loc['FinancingCashFlow']
        # stock_summary['Avg_Gross_margin'] = self.avg_gross_margin
        # stock_summary['Avg_sales_growth'] = self.avg_sales_growth
        # stock_summary['Avg_ebit_margin'] = self.avg_ebit_margin
        # stock_summary['Avg_ebit_growth'] = self.avg_ebit_growth
        # stock_summary['Avg_net_margin'] = self.avg_net_margin
        # stock_summary['Avg_NetIncome_growth'] = self.avg_ni_growth
        # stock_summary['Years_of_data'] = self.years_of_data

        return stock_summary

    def csv_statements(self, df):
        """Export the DataFrame in csv format.

        :param df: a DataFrame containing stock data
        """

        df.to_csv(f'{self.asset_code}_{df.name}.csv', sep=',', encoding='utf-8')

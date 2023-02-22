from smart_value.asset import *
import pandas as pd


class Stock(Asset):
    """a type of Assets"""

    def __init__(self, symbol):
        """
        :param symbol: string ticker of the stock
        """
        super().__init__(symbol)

        self.shares = None
        self.excess_return = None
        self.ideal_price = None
        self.fcf_value = None
        self.navps = None
        self.realizable_value = None
        self.nonop_assets = None
        self.last_dividend = None  # dividend for stocks and coupon for bonds
        self.sector = None
        self.report_currency = None
        self.annual_bs = None  # annual balance sheet data
        self.quarter_bs = None  # last quarter balance sheet data
        self.cf_df = None
        self.is_df = None
        self.fx_rate = None
        self.buyback = None
        self.last_fy = None  # last coupon date for bonds
        self.most_recent_quarter = None

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
        stock_summary.insert(loc=0, column='Ticker', value=self.symbol)
        stock_summary.insert(loc=1, column='Name', value=self.name)
        stock_summary.insert(loc=2, column='Sector', value=self.sector)
        stock_summary.insert(loc=3, column='Exchange', value=self.exchange)
        stock_summary.insert(loc=4, column='Price', value=self.price[0])
        stock_summary.insert(loc=5, column='Price_currency', value=self.price[1])
        stock_summary.insert(loc=6, column='Shares', value=self.shares)
        stock_summary.insert(loc=7, column='Reporting_Currency', value=self.report_currency)
        stock_summary.insert(loc=8, column='Fx_rate', value=self.fx_rate)
        stock_summary.insert(loc=9, column='Dividend', value=self.last_dividend)
        stock_summary.insert(loc=10, column='Buyback', value=self.buyback)
        stock_summary.insert(loc=11, column='Last_fy', value=self.last_fy)
        stock_summary['CFO'] = self.cf_df.loc['OperatingCashFlow']
        stock_summary['CFI'] = self.cf_df.loc['InvestingCashFlow']
        stock_summary['CFF'] = self.cf_df.loc['FinancingCashFlow']
        return stock_summary

    def csv_statements(self, df):
        """Export the DataFrame in csv format.

        :param df: a DataFrame containing stock data
        """

        df.to_csv(f'{self.symbol}_{df.name}.csv', sep=',', encoding='utf-8')

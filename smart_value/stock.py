from smart_value.asset import *
from smart_value.financial_data import yahoo_data as yh
from smart_value.financial_data import exchange_rate as fx


class Stock(Asset):
    """a type of Securities"""

    def __init__(self, security_code, source="yf"):
        """
        :param security_code: string ticker of the stock
        :param source: "yf" chooses yahoo finance
        """
        super().__init__(security_code)

        self.invest_horizon = 3  # 3 years holding period for stock by default
        self.report_currency = None
        self.is_df = None
        self.bs_df = None
        self.fx_rate = None
        self.source = source
        self.load_data()

    def load_data(self):
        """data source selector."""

        if self.source == "yf":
            self.load_from_yf()
        else:
            pass  # Other sources of data to-be-implemented

    def load_from_yf(self):
        """Scrap the financial_data from yfinance API"""

        ticker_data = yh.Financials(self.asset_code)

        self.name = ticker_data.name
        self.price = ticker_data.price
        self.exchange = ticker_data.exchange
        self.shares = ticker_data.shares
        self.report_currency = ticker_data.report_currency
        self.fx_rate = fx.get_forex_rate(self.report_currency, self.price[1])
        self.periodic_payment = ticker_data.dividends
        self.is_df = ticker_data.income_statement
        self.bs_df = ticker_data.balance_sheet

    def present_data(self):
        """ Return a summary of all the key stock data.

        :return: All key stock data in one DataFrame
        """

        # balance sheet information
        stock_summary = self.bs_df.iloc[:, :1]
        # dividend
        stock_summary.insert(loc=-1, column='', value=self.periodic_payment)

        return stock_summary

    def csv_statements(self, df):
        """Export the DataFrame in csv format.

        :param df: a DataFrame containing stock data
        """

        df.to_csv(f'{self.asset_code}_{df.name}.csv', sep=',', encoding='utf-8')

import pathlib
import pandas as pd
from yfinance import Tickers, Ticker, download
import datetime as dt
import time
import random
import smart_value.tools.stock_screener as screener

'''
Available yfinance features:
attrs = [
    'info', 'financials', 'quarterly_financials', 'major_holders',
    'institutional_holders', 'balance_sheet', 'quarterly_balance_sheet',
    'cashflow', 'quarterly_cashflow', 'earnings', 'quarterly_earnings',
    'sustainability', 'recommendations', 'calendar'
]
'''

cwd = pathlib.Path.cwd().resolve()
screener_folder = cwd / 'financial_models' / 'Opportunities' / 'Screener'
json_dir = screener_folder / 'data'

failure_list = []


def get_yf_data(symbols):
    """
    Download the stock data and export them into 4 json files:
    1. intro_data, 2. bs_data, 3. is_data. 4. cf_data

    :param symbols: list of symbols separated by a space

    PS. yfinance marks a table as abusive if it seems to be hogging its resources, or consistently taking more than 30s
    to execute.
    """

    full_list = symbols.split(" ")
    while full_list:
        pop_num = 3
        n = random.randint(pop_num * 5, pop_num * 30)
        # select the first few items
        selected_list = full_list[:pop_num]
        # del the first few items
        del full_list[0:pop_num]
        print(f"downloading data from {selected_list[0]} to {selected_list[-1]}, {len(full_list)} waiting to be "
              f"processed...")
        selected_str = ' '.join(selected_list)
        download_yf(selected_str, 0)
        # wait before next batch to avoid being marked as abusive by yfinance
        wait = n
        print(f"wait {wait} seconds to avoid being marked as abusive...")
        time.sleep(wait)
    if failure_list:
        print(f"failed list: {' '.join(failure_list)}")


def download_yf(symbols, attempt):
    """
        Download the stock data and export them into 4 json files:
        1. intro_data, 2. bs_data, 3. is_data. 4. cf_data

        :param attempt: try_count
        :param symbols: list of symbols separated by a space
        """

    # external API error re-try
    max_try = 2

    info_col = ['shortName', 'sector', 'industry', 'market', 'sharesOutstanding', 'financialCurrency',
                'lastFiscalYearEnd', 'mostRecentQuarter']
    yf_companies = Tickers(symbols)
    symbol_list = symbols.split(" ")
    while symbol_list:
        symbol = symbol_list.pop(0)  # pop from the beginning
        try:
            # introductory information
            info = pd.Series(yf_companies.tickers[symbol].info)
            # info['currency'] = companies.tickers[symbol].fast_info['currency']  # get it when updating price
            # info['exchange'] = companies.tickers[symbol].fast_info['exchange']  # Use market instead
            info = info.loc[info_col]
            # Balance Sheet
            bs_df = yf_companies.tickers[symbol].quarterly_balance_sheet
            bs_df = format_data(bs_df)

            # Income statement
            is_df = yf_companies.tickers[symbol].financials
            is_df = format_data(is_df)
            # # Cash Flow statement
            cf_df = yf_companies.tickers[symbol].cashflow
            cf_df = format_data(cf_df)
            # Create one big one-row stock_df by using side-by-side merge
            stock_df = pd.concat([info, bs_df, is_df, cf_df])
            stock_df = stock_df.transpose()
            stock_df['ticker'] = symbol
            stock_df.set_index('ticker').to_json(json_dir / f"{symbol}_data.json")
            if attempt == 0:
                print(f"{symbol} exported, {len(symbol_list)} stocks left...")
            else:
                print(f"Retry succeeded: {symbol} exported")
        except:
            error_str = symbol
            attempt += 1
            if attempt <= max_try:
                print(f"API error - {error_str}. Re-try in 60 sec: {max_try - attempt} "
                      f"attempts left...")
                time.sleep(60)
                download_yf(error_str, attempt)
            else:
                failure_list.append(error_str)
                print(f'API error - {error_str} failed after {max_try} attempts')
            # restart counting at next ticker
            attempt = 0
            continue


def format_data(df):
    """ Return the formatted one row annual statement dataframe.

    :param df: Dataframe
    :return: formatted Dataframe
    """

    if len(df.columns) <= 1:
        df.columns = [0]
        return df
    else:
        col = df.index.values.tolist()
        first = df.iloc[:, :1]
        first.columns = [0]
        second = df.iloc[:, 1:2]
        second.columns = [0]
        second.index = [s + "_-1" for s in col]
        return pd.concat([second, first])


def get_forex(report_symbol, price_symbol):
    """ Return the forex quote
    Known bug: If there is no quote available, the fxRate will be 0.

    :param report_symbol: String forex report symbol
    :param price_symbol: String forex price symbol
    :return: Float average of last 3 day's forex quote
    """

    start_date = dt.datetime.today() - dt.timedelta(days=7)
    end_date = dt.datetime.today()
    # print(report_symbol + " " + price_symbol)
    if report_symbol == price_symbol:
        return 1
    elif report_symbol == "MOP" and price_symbol == "HKD":
        # Known missing quote on yahoo finance
        return 0.98
    else:
        forex_code = f"{report_symbol}{price_symbol}=X"
        # print(forex_code)
        # return the average of the last 3 forex quote
        return download(forex_code, start_date, end_date).tail(3)['Adj Close'].mean()


def get_quote(symbol, option):
    """ Get the updated quote from yfinance fast_info

    :param symbol: string stock symbol
    :param option: fast_info argument
    Below available
    'currency', 'dayHigh', 'dayLow', 'exchange', 'fiftyDayAverage', 'lastPrice', 'lastVolume',
    'marketCap', 'open', 'previousClose', 'quoteType', 'regularMarketPreviousClose', 'shares', 'tenDayAverageVolume',
    'threeMonthAverageVolume', 'timezone', 'twoHundredDayAverage', 'yearChange', 'yearHigh', 'yearLow'
    :return: quote given option
    """

    company = Ticker(symbol).fast_info
    return company[option]


def get_info(symbol, option):
    """ Get the updated quote from yfinance info

    :param symbol: string stock symbol
    :param option: info argument
    Below available
    'sector', 'fullTimeEmployees', 'longBusinessSummary', 'city', 'phone', 'country', 'companyOfficers', 'website',
    'maxAge', 'address1', 'industry', 'address2', 'ebitdaMargins', 'profitMargins', 'grossMargins', 'operatingCashflow',
     'revenueGrowth', 'operatingMargins', 'ebitda', 'targetLowPrice', 'recommendationKey', 'grossProfits',
     'freeCashflow', 'targetMedianPrice', 'earningsGrowth', 'currentRatio', 'returnOnAssets',
     'numberOfAnalystOpinions', 'targetMeanPrice', 'debtToEquity', 'returnOnEquity', 'targetHighPrice', 'totalCash',
     'totalDebt', 'totalRevenue', 'totalCashPerShare', 'financialCurrency', 'revenuePerShare', 'quickRatio',
     'recommendationMean', 'shortName', 'longName', 'isEsgPopulated', 'gmtOffSetMilliseconds', 'messageBoardId',
     'market', 'annualHoldingsTurnover', 'enterpriseToRevenue', 'beta3Year', 'enterpriseToEbitda',
     'morningStarRiskRating', 'forwardEps', 'revenueQuarterlyGrowth', 'sharesOutstanding', 'fundInceptionDate',
     'annualReportExpenseRatio', 'totalAssets', 'bookValue', 'sharesShort', 'sharesPercentSharesOut', 'fundFamily',
     'lastFiscalYearEnd', 'heldPercentInstitutions', 'netIncomeToCommon', 'trailingEps', 'lastDividendValue',
     'SandP52WeekChange', 'priceToBook', 'heldPercentInsiders', 'nextFiscalYearEnd', 'yield', 'mostRecentQuarter',
     'shortRatio', 'sharesShortPreviousMonthDate', 'floatShares', 'beta', 'enterpriseValue', 'priceHint',
     'threeYearAverageReturn', 'lastSplitDate', 'lastSplitFactor', 'legalType', 'lastDividendDate',
     'morningStarOverallRating', 'earningsQuarterlyGrowth', 'priceToSalesTrailing12Months', 'dateShortInterest',
     'pegRatio', 'ytdReturn', 'forwardPE', 'lastCapGain', 'shortPercentOfFloat', 'sharesShortPriorMonth',
     'impliedSharesOutstanding', 'category', 'fiveYearAverageReturn', 'trailingAnnualDividendYield', 'payoutRatio',
     'navPrice', 'trailingAnnualDividendRate', 'toCurrency', 'expireDate', 'algorithm', 'dividendRate',
     'exDividendDate', 'circulatingSupply', 'startDate', 'trailingPE', 'lastMarket', 'maxSupply', 'openInterest',
     'volumeAllCurrencies', 'strikePrice', 'ask', 'askSize', 'fromCurrency', 'fiveYearAvgDividendYield', 'bid',
     'tradeable', 'dividendYield', 'bidSize', 'coinMarketCapLink', 'preMarketPrice', 'logo_url', 'trailingPegRatio']

    :return: quote given option
    """

    company = Ticker(symbol).info
    return company[option]


def update_data(data):
    """Update the price and currency info"""

    ticker = data.index.values.tolist()[0]
    data['price'] = get_quote(ticker, 'last_price')
    data['priceCurrency'] = get_quote(ticker, 'currency')
    # Forex
    data['fxRate'] = get_forex(data['financialCurrency'].values[:1][0], data['priceCurrency'].values[:1][0])
    # exclude minor countries without forex data
    # data = data[data['fxRate'].notna()]
    return data


def clean_data(df):
    """Clean up the dataframe"""

    # Exclude the rows that has missing financial data
    df = df[df['Total Assets'].notna() & df['financialCurrency'].notna()]
    # print(df.to_string())
    # Preparing the data
    df['lastFiscalYearEnd'] = pd.to_datetime(df['lastFiscalYearEnd'])
    df['lastDividend'] = - df['Cash Dividends Paid'] / df['sharesOutstanding']
    df['lastBuyback'] = - df['Repurchase Of Capital Stock'] / df['sharesOutstanding']
    df['NoncurrentLiability'] = df['Total Assets'] - df['Total Equity Gross Minority Interest']
    df['NoncurrentLiability_-1'] = df['Total Assets_-1'] - df['Total Equity Gross Minority Interest_-1']
    df['NoncommonInterest'] = df['Total Equity Gross Minority Interest'] - df['Common Stock Equity']
    df['NoncommonInterest_-1'] = df['Total Equity Gross Minority Interest_-1'] - df['Common Stock Equity_-1']

    df = df.fillna(0)
    df = df[
        ['shortName', 'sector', 'industry', 'market', 'price', 'priceCurrency', 'sharesOutstanding',
         'financialCurrency', 'fxRate', 'lastFiscalYearEnd', 'mostRecentQuarter', 'lastDividend', 'lastBuyback',
         'Total Assets', 'Current Assets', 'Current Liabilities',
         'Total Assets_-1', 'Current Assets_-1', 'Current Liabilities_-1',
         'Cash And Cash Equivalents', 'Other Short Term Investments',
         'Cash And Cash Equivalents_-1', 'Other Short Term Investments_-1',
         'Current Debt And Capital Lease Obligation', 'Current Capital Lease Obligation',
         'Current Debt And Capital Lease Obligation_-1', 'Current Capital Lease Obligation_-1',
         'Long Term Debt And Capital Lease Obligation', 'Long Term Capital Lease Obligation',
         'Long Term Debt And Capital Lease Obligation_-1', 'Long Term Capital Lease Obligation_-1',
         'Total Equity Gross Minority Interest', 'Common Stock Equity',
         'Total Equity Gross Minority Interest_-1', 'Common Stock Equity_-1',
         'Investment Properties', 'Long Term Equity Investment', 'Investmentin Financial Assets',
         'Investment Properties_-1', 'Long Term Equity Investment_-1', 'Investmentin Financial Assets_-1',
         'Net PPE', 'Total Revenue', 'Cost Of Revenue', 'Selling General And Administration',
         'Net PPE_-1', 'Total Revenue_-1', 'Cost Of Revenue_-1', 'Selling General And Administration_-1',
         'Net Income Common Stockholders', 'Interest Paid Cfo', 'Interest Paid Cff',
         'Net Income Common Stockholders_-1', 'Interest Paid Cfo_-1', 'Interest Paid Cff_-1',
         'Operating Cash Flow', 'Investing Cash Flow', 'Financing Cash Flow', 'End Cash Position',
         'Operating Cash Flow_-1', 'Investing Cash Flow_-1', 'Financing Cash Flow_-1', 'End Cash Position_-1']]

    return df


def output_data():
    """Merge, clean, standardize names, and export data from yfinance"""

    df = screener.merge_data("yf")
    df = clean_data(df)
    screener.export_data(df)


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
        self.sector = self.stock_data.info['sector']
        self.price = [self.stock_data.fast_info['last_price'], self.stock_data.fast_info['currency']]
        self.exchange = self.stock_data.fast_info['exchange']
        self.shares = self.stock_data.info['sharesOutstanding']
        self.report_currency = self.stock_data.info['financialCurrency']
        self.avg_gross_margin = None
        self.avg_ebit_margin = None
        self.avg_net_margin = None
        self.avg_sales_growth = None
        self.avg_ebit_growth = None
        self.avg_ni_growth = None
        self.years_of_data = None
        self.annual_bs = self.get_balance_sheet("annual")
        self.quarter_bs = self.get_balance_sheet("quarter")
        self.income_statement = self.get_income_statement()
        self.cash_flow = self.get_cash_flow()
        if self.stock_data.info['mostRecentQuarter'] is None:
            self.most_recent_quarter = pd.to_datetime(dt.datetime.fromtimestamp(self.stock_data.info['lastFiscalYearEnd'])
                                                      .strftime("%Y-%m-%d"))
        else:
            self.most_recent_quarter = pd.to_datetime(dt.datetime.fromtimestamp(self.stock_data.info['mostRecentQuarter'])
                                                      .strftime("%Y-%m-%d"))
        try:
            self.dividends = -int(self.cash_flow.loc['CashDividendsPaid'][0]) / self.shares
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
        # Ending of Cleaning: drop the dummy column after join
        is_df.drop('Dummy', inplace=True, axis=1)
        is_df = is_df.fillna(0).transpose()
        try:
            is_df['Gross_margin'] = is_df['CostOfRevenue'] / is_df['TotalRevenue'] * 100
            is_df['Gross_margin'] = is_df['Gross_margin'].astype(float).round(decimals=2)
        except ZeroDivisionError:
            is_df['Gross_margin'] = 0
        try:
            is_df['Ebit'] = is_df['TotalRevenue'] - is_df['CostOfRevenue'] - is_df['SellingGeneralAndAdministration']
        except ZeroDivisionError:
            is_df['Ebit'] = 0
        try:
            is_df['Ebit_margin'] = is_df['Ebit'] / is_df['TotalRevenue'] * 100
            is_df['Ebit_margin'] = is_df['Ebit_margin'].astype(float).round(decimals=2)
        except ZeroDivisionError:
            is_df['Ebit_margin'] = 0
        try:
            is_df['Net_margin'] = is_df['NetIncomeCommonStockholders'] / is_df['TotalRevenue'] * 100
            is_df['Net_margin'] = is_df['Net_margin'].astype(float).round(decimals=2)
        except ZeroDivisionError:
            is_df['Net_margin'] = 0

        self.avg_gross_margin = is_df["Gross_margin"].mean()
        self.avg_ebit_margin = is_df["Ebit_margin"].mean()
        self.avg_net_margin = is_df["Net_margin"].mean()
        try:
            self.avg_sales_growth = round((is_df.iloc[::-1]['TotalRevenue']
                                           .pct_change().dropna()).mean().astype(float) * 100, 2)
        except AttributeError:
            # empty TotalRevenue bug
            self.avg_sales_growth = 0
        try:
            self.avg_ebit_growth = round((is_df.iloc[::-1]['Ebit']
                                          .pct_change().dropna()).mean().astype(float) * 100, 2)
        except AttributeError:
            self.avg_ebit_growth = 0
        try:
            self.avg_ni_growth = round((is_df.iloc[::-1]['NetIncomeCommonStockholders']
                                        .pct_change().dropna()).mean().astype(float) * 100, 2)
        except AttributeError:
            self.avg_ni_growth = 0
        self.years_of_data = len(is_df['TotalRevenue'])

        return is_df.transpose()

    def get_cash_flow(self):
        """Returns a DataFrame with selected Cash flow statement data"""

        cash_flow = self.stock_data.get_cashflow()
        # Start of Cleaning: make sure the data has all the required indexes
        dummy = {"Dummy": [None, None, None, None, None]}
        cf_index = ['OperatingCashFlow', 'InvestingCashFlow', 'FinancingCashFlow',
                    'CashDividendsPaid', 'RepurchaseOfCapitalStock']
        dummy_df = pd.DataFrame(dummy, index=cf_index)
        clean_cf = dummy_df.join(cash_flow)
        cf_df = clean_cf.loc[cf_index]
        # Ending of Cleaning: drop the dummy column after join
        cf_df.drop('Dummy', inplace=True, axis=1)

        return cf_df.fillna(0)

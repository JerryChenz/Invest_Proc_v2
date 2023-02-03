import pathlib
import pandas as pd
from yfinance import Tickers
from datetime import datetime
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

info_col = ['ticker', 'shortName', 'sector', 'industry', 'market', 'sharesOutstanding', 'financialCurrency',
            'lastFiscalYearEnd', 'mostRecentQuarter']


def download_yf(symbols):
    """
    Download the stock data and export them into 4 json files:
    1. intro_data, 2. bs_data, 3. is_data. 4. cf_data

    :param symbols: list of symbols separated by a space
    """
    print("downloading data...")
    companies = Tickers(symbols)
    symbol_list = symbols.split(" ")

    print("export begins...")
    while symbol_list:
        symbol = symbol_list.pop(0)  # pop from the beginning

        # introductory information
        info = pd.Series(companies.tickers[symbol].info)
        # info['currency'] = companies.tickers[symbol].fast_info['currency']  # get it when updating price
        # info['exchange'] = companies.tickers[symbol].fast_info['exchange']  # Use market instead
        info['ticker'] = symbol
        info['download_date'] = datetime.today().strftime('%Y-%m-%d')
        # Balance Sheet
        bs_df = companies.tickers[symbol].quarterly_balance_sheet
        # Income statement
        is_df = companies.tickers[symbol].financials
        # Cash Flow statement
        cf_df = companies.tickers[symbol].cashflow
        stock_df = pd.concat([info.loc[info_col], format_quarter(bs_df), format_annual(is_df), format_annual(cf_df)])
        stock_df.to_json(json_dir / f"{symbol}_data.json")
        print(f"{symbol} exported, {len(symbol_list)} stocks left...")


def prepare_screener(symbols):
    """Collect company data from the json files, then export them into a csv.

    :param symbols: String symbols separated by a space
    """

    screener.collect_files(symbols)


def format_quarter(df):
    col = df.index.values.tolist()
    first = df.iloc[:, :1]
    first.columns = [0]
    second = df.iloc[:, 1:2]
    second.index = [s + "_-1" for s in col]
    second.columns = [0]
    return pd.concat([second, first])


def format_annual(df):
    """ Return the formatted one row annual statement dataframe.

    :param df: Dataframe
    :param col: List with annual statement column names
    :return: formatted Dataframe
    """

    col = df.index.values.tolist()
    first = df.iloc[:, :1]
    first.columns = [0]
    second = df.iloc[:, 1:2]
    second.columns = [0]
    second.index = [s + "_-1" for s in col]
    third = df.iloc[:, 2:3]
    third.columns = [0]
    third.index = [s + "_-2" for s in col]

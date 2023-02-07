import pathlib
import pandas as pd
from yfinance import Tickers
import time
import random
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
        n = random.randint(pop_num*5, pop_num*20)
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
                print(f"API error - {error_str}. Re-try in 60 sec: {max_try-attempt} "
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
    :param col: List with annual statement column names
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

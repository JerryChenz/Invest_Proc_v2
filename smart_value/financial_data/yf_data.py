import pathlib
import pandas as pd
from yfinance import Tickers
from datetime import datetime
from smart_value.tools.stock_screener import collect_files, merge_data

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

def download_yf(symbols):

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
        info['download_date'] = datetime.today().strftime('%Y-%m-%d')
        info.to_json(json_dir / f'{symbol}_intro_data.json')
        # Balance Sheet
        companies.tickers[symbol].quarterly_balance_sheet.to_json(json_dir / f'{symbol}_bs_data.json')
        # Income statement
        companies.tickers[symbol].financials.to_json(json_dir / f'{symbol}_is_data.json')
        # Cash Flow statement
        companies.tickers[symbol].cashflow.to_json(json_dir / f'{symbol}_cf_data.json')
        print(f"{symbol} exported, {len(symbol_list)} stocks left...")

def prepare_company(symbols):
    """Collect a list of company files, then export json.

    :param symbols: String symbols separated by a space
    :return
    """

    symbol_list = symbols.split(" ")

    while symbol_list:
        print("Begins to prepare screening data...")
        symbol = symbol_list.pop(0)  # pop from the beginning
        try:
            intro_df = collect_files(symbol)[0]
            # Quarterly Balance sheet statement
            bs_df = collect_files(symbol)[1]
            # Annual Income statement
            is_df = collect_files(symbol)[2]
            # Annual Cash flor statement
            cf_df = collect_files(symbol)[3]

            return pd.concat(format_intro(intro_df), format_bs(bs_df), format_bs(is_df), format_bs(cf_df))

        except RuntimeError:
            print(f"{symbol} Data Incomplete. Skip. {len(symbol_list)} stocks left to prepare...")
            continue

def format_intro(df):

    df = df[['shortName']]
    return df

def format_bs(df):
    return df

def format_is(df):
    return df

def format_cf(df):
    return df





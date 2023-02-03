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
        info.to_json(json_dir / f'{symbol}_intro_data.json')
        # Balance Sheet
        companies.tickers[symbol].quarterly_balance_sheet.to_json(json_dir / f'{symbol}_bs_data.json')
        # Income statement
        companies.tickers[symbol].financials.to_json(json_dir / f'{symbol}_is_data.json')
        # Cash Flow statement
        companies.tickers[symbol].cashflow.to_json(json_dir / f'{symbol}_cf_data.json')
        print(f"{symbol} exported, {len(symbol_list)} stocks left...")


def prepare_screener(symbols):
    """Collect company data from the json files, then export them into a csv.

    :param symbols: String symbols separated by a space
    """

    intro_col = ['ticker', 'shortName', 'sector', 'industry', 'market', 'sharesOutstanding', 'financialCurrency',
                 'lastFiscalYearEnd', 'mostRecentQuarter']
    screener.collect_files(symbols, intro_col)



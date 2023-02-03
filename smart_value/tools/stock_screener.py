import smart_value.stock as stock
import pandas as pd
import pathlib
import time
import glob, os

'''
Two ways to create a screener:
1. Create a filter while gathering the data.
2. Collect data & filter based on multiple criteria afterwards. (preferred and used here)
'''

cwd = pathlib.Path.cwd().resolve()
screener_folder = cwd / 'financial_models' / 'Opportunities' / 'Screener'
json_dir = screener_folder / 'data'


# Step 1: Collect files
def collect_files(symbols, intro_col, bs_col, is_col, cf_col):
    """Find and read the 4 jsons of a company, then export to a screener_data csv

    :param intro_col: List with intro column names
    :param bs_col: List with bs column names
    :param is_col: List with is column names
    :param cf_col: List with cf column names
    :param symbols: String symbols separated by a space
    :return
    """

    screener_dfs = []  # an empty list to store the dataframes
    symbol_list = symbols.split(" ")

    while symbol_list:
        print("Begins to prepare screening data...")
        symbol = symbol_list.pop(0)  # pop from the beginning

        try:
            stock_df = read_company_json(symbol, intro_col, bs_col, is_col, cf_col)
            screener_dfs.append(stock_df.transpose())  # a single row
        except RuntimeError:
            print(f"{symbol} Data Incomplete. Skip. {len(symbol_list)} stocks left to prepare...")
            continue
    df = pd.concat(screener_dfs, ignore_index=False)  # concatenate all the data frames in the list.
    # df = df.set_index('Ticker')  # Not needed I think
    df.to_csv(screener_folder / 'screener_data.csv')


def read_company_json(symbol, intro_col, bs_col, is_col, cf_col):
    """ read the 4 jsons of a company

    :param intro_col: List with intro column names
    :param bs_col: List with bs column names
    :param is_col: List with is column names
    :param cf_col: List with cf column names
    :param symbol: string
    :return: a list of 4 pandas
    """

    this_intro = json_dir / f'{symbol}_intro_data.json'
    this_bs = json_dir / f'{symbol}_bs_data.json'
    this_is = json_dir / f'{symbol}_is_data.json'
    this_cf = json_dir / f'{symbol}_cf_data.json'
    if this_intro.is_file() and this_bs.is_file() and this_is.is_file() and this_cf.is_file():
        intro_df = pd.read_json(this_intro)
        # Quarterly Balance sheet statement
        bs_df = pd.read_json(this_bs)
        # Annual Income statement
        is_df = pd.read_json(this_is)
        # Annual Cash flor statement
        cf_df = pd.read_json(this_cf)
        return pd.concat([intro_df.loc[intro_col], bs_df.iloc[:, :1].loc[bs_col],
                         format_annual(is_df, is_col), format_annual(cf_df, cf_col)])
    else:
        raise RuntimeError(f"{symbol} Data Incomplete.")


def format_annual(df, col):
    """ Return the formatted one row annual statement dataframe.

    :param df: Dataframe
    :param col: List with annual statement column names
    :return: formatted Dataframe
    """

    first = df.iloc[:, :1].loc[col]
    first.columns = [0]
    second = df.iloc[:, 1:2].loc[col]
    second.columns = [0]
    second.index = [s + "_-1" for s in col]
    third = df.iloc[:, 2:3].loc[col]
    third.columns = [0]
    third.index = [s + "_-2" for s in col]

    return pd.concat([third, second, first])


# Step 2: filter done using ipynb with Jupyter Notebook

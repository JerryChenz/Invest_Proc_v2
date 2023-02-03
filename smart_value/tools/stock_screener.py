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
def collect_files(symbol):
    """Collect a list of company files, then export json.

    :param symbol: String symbol
    :return a list of pandas
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
        return [intro_df, bs_df, is_df, cf_df]
    else:
        raise RuntimeError(f"{symbol} Data Incomplete.")

def company_data(ticker, source):
    """Return the Dataframe of a company's data.

    :param ticker: collect the data using string ticker
    :return Dataframe
    """




def merge_data():
    """Merge multiple JSON files into a pandas DataFrame, then export to csv"""

    json_pattern = os.path.join(json_dir, '*.json')
    files = glob.glob(json_pattern)

    dfs = []  # an empty list to store the data frames
    for file in files:
        data = pd.read_json(file)  # read data frame from json file
        dfs.append(data.transpose())  # append the data frame to the list
    df = pd.concat(dfs, ignore_index=False)  # concatenate all the data frames in the list.
    df = df.set_index('Ticker')
    df.to_csv(screener_folder / 'screener_summary.csv')

# Step 2: filter done using ipynb with Jupyter Notebook

import smart_value.stock
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


# Step 1: Collect data
def collect_data(tickers, source):
    """Collect a list of company data, then export json.

    :param tickers: a list of tickers to screen through
    :param source: "yf" chooses yahoo finance
    :return failed_list
    """

    failed_list = []

    # Collect the company data and export in json
    while tickers:
        ticker = tickers.pop()
        try:
            no_error = company_data(ticker, source, 0)
            if no_error or no_error is None:

                print(ticker + ' data added.')
            else:
                failed_list.append(ticker)
                print(f"failed list: {failed_list}")
        except KeyError:
            print(f'{ticker} is not valid, skip')
            continue
    # export the json files and export the summary in DataFrame
    print("merging data...")
    merge_data()
    print("csv exported.")
    return failed_list


def company_data(ticker, source, attempt):
    """Collect the company data, then export json.

    :param attempt: try_count
    :param ticker: collect the data using string ticker
    :param source: "yf" chooses yahoo finance
    :return False if the call failed, True otherwise
    """

    # external API error re-try
    max_try = 3

    try:
        company = smart_value.stock.Stock(ticker, source)
        # export the summary
        new_row = company.current_summary().transpose()
        new_row.to_json(json_dir / f'{ticker} data.json')
        time.sleep(3)
    except IndexError:
        attempt += 1
        if attempt <= max_try:
            print(f'external API error, will re-try {ticker} after 80 sec')
            time.sleep(80)
            print(f're-try {ticker}, attempt {attempt}')
            company_data(ticker, source, attempt)
        else:
            print(f'external API error, {ticker} failed after {attempt} attempts')
            return False
    except ValueError:
        attempt += 1
        if attempt <= max_try:
            print(f'external API error, will re-try {ticker} after 120 sec')
            time.sleep(120)
            print(f're-try {ticker}, attempt {attempt}')
            company_data(ticker, source, attempt)
        else:
            print(f'external API error, {ticker} failed after {attempt} attempts')
            return False
    except TypeError:
        attempt += 1
        if attempt <= max_try:
            print(f'external API error, will re-try {ticker} after 120 sec')
            time.sleep(120)
            print(f're-try {ticker}, attempt {attempt}')
            company_data(ticker, source, attempt)
        else:
            print(f'external API error, {ticker} failed after {attempt} attempts')
            return False
    except AttributeError:
        attempt += 1
        if attempt <= max_try:
            print(f'external API error, will re-try {ticker} after 120 sec')
            time.sleep(120)
            print(f're-try {ticker}, attempt {attempt}')
            company_data(ticker, source, attempt)
        else:
            print(f'external API error, {ticker} failed after {attempt} attempts')
            return False
    else:
        return True


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

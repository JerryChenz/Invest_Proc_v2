import glob
import os
import pathlib
import pandas as pd
import time
import random
# from smart_value.tools.helpers import remove_files
from smart_value.financial_data import yf_data as yf
from smart_value.financial_data import yq_data as yq

'''
Two ways to create a screener:
1. Create a filter while gathering the data.
2. Collect data & filter based on multiple criteria afterwards. (preferred and used here)

# Actual Screening will be done using ipynb with Jupyter Notebook
'''

cwd = pathlib.Path.cwd().resolve()
screener_folder = cwd / 'financial_models' / 'Opportunities' / 'Screener'
json_dir = screener_folder / 'data'


def get_data(symbols, source):
    """
    Download the stock data and export them into 4 json files:
    1. intro_data, 2. bs_data, 3. is_data. 4. cf_data

    :param symbols: list of symbols separated by a space
    :param source: String Data Source selector

    PS. yfinance marks a table as abusive if it seems to be hogging its resources, or consistently taking more than 30s
    to execute.
    """

    failure_list = []

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
        if source == "yf":
            failure_list = yf.download_yf(selected_str, 0, failure_list)
            # wait before next batch to avoid being marked as abusive by yfinance
            wait = n
            print(f"wait {wait} seconds to avoid being marked as abusive...")
            time.sleep(wait)
        elif source == "yq":
            failure_list = yq.download_yq(selected_str, 0, failure_list)
            # wait before next batch to avoid being marked as abusive by yfinance
            wait = n
            print(f"wait {wait} seconds to avoid being marked as abusive...")
            time.sleep(wait)
    if failure_list:
        print(f"failed list: {' '.join(failure_list)}")


def output_data():
    """Merge multiple JSON files into a pandas DataFrame, then export to csv"""

    json_pattern = os.path.join(json_dir, '*.json')
    files_path = glob.glob(json_pattern)  # gets back a list of file objects
    dfs = []
    files_count = len(files_path)
    i = 0  # Counter for progress

    # Step 2: merge the data files
    for file_path in files_path:
        i += 1
        print(f"processing the {os.path.basename(file_path)},{i}/{files_count} files...")
        file_data = pd.read_json(file_path)
        dfs.append(file_data)  # append the data frame to the list
    print("Merging, cleaning, and exporting data. Please wait...")
    merged_df = pd.concat(dfs, ignore_index=False)

    # Step 3: clean the data
    if 'source' in merged_df.columns:
        try:
            if merged_df['source'] == "yf_data":
                merged_df = yf.clean_data(merged_df)
            elif merged_df['source'] == "yq_data":
                merged_df = yq.clean_data(merged_df)
            else:
                raise ValueError
        except ValueError:
            print("Unrecognizable data file source.")
    else:
        merged_df = yf.clean_data(merged_df)

    # Step 4: export the data
    export_data(merged_df)

# Decommissioned code because the yahoo finance price updates are not efficient enough
# def prepare_data(path, source):
#     """prepare the data from json before cleaning
#
#     :param source: string data source selector
#     :param path: path of the json file
#     """
#
#     # prepare the data in batches to prevent abusing the API or crash
#     data = pd.read_json(path)  # read data frame from json file
#     if source == "yf":
#         # print(data)
#         data = yf.update_data(data)
#     elif source == "yq":
#         data = yq.update_data(data)
#     else:
#         data = data
#     # print(data)
#     return data


def export_data(df):
    """Export the df in csv format after the column names are standardized"""

    standardized_names = ['shortName', 'sector', 'industry', 'market', 'priceCurrency', 'sharesOutstanding',
                          'reportCurrency', 'lastFiscalYearEnd', 'mostRecentQuarter', 'lastDividend',
                          'lastBuyback',
                          'totalAssets', 'currentAssets', 'currentLiabilities',
                          'totalAssets_-1', 'currentAssets_-1', 'currentLiabilities_-1',
                          'cashAndCashEquivalents', 'otherShortTermInvestments',
                          'cashAndCashEquivalents_-1', 'otherShortTermInvestments_-1',
                          'currentDebtAndCapitalLease', 'currentCapitalLease',
                          'currentDebtAndCapitalLease_-1', 'currentCapitalLease_-1',
                          'longTermDebtAndCapitalLease', 'longTermCapitalLease',
                          'longTermDebtAndCapitalLease_-1', 'longTermCapitalLease_-1',
                          'totalEquityAndMinorityInterest', 'commonStockEquity',
                          'totalEquityAndMinorityInterest_-1', 'commonStockEquity_-1',
                          'investmentProperties', 'longTermEquityInvestment', 'longTermFinancialAssets',
                          'investmentProperties_-1', 'longTermEquityInvestment_-1', 'longTermFinancialAssets_-1',
                          'netPPE', 'totalRevenue', 'costOfRevenue', 'sellingGeneralAndAdministration',
                          'netPPE_-1', 'totalRevenue_-1', 'costOfRevenue_-1', 'sellingGeneralAndAdministration_-1',
                          'interestExpense', 'netIncomeCommonStockholders',
                          'interestExpense_-1', 'netIncomeCommonStockholders_-1',
                          'cfo', 'cfi', 'cff', 'endCashPosition',
                          'cfo_-1', 'cfi_-1', 'cff_-1', 'endCashPosition_-1']

    df.columns = standardized_names
    df.to_csv(screener_folder / 'screener_summary.csv')

# -*- coding: utf-8 -*-
"""
Get fundamental data
@author: Adam Getbags
Data provided by Financial Modeling Prep
https://site.financialmodelingprep.com/developer/docs/
Python API wrapper by
https://github.com/JerBouma/FundamentalAnalysis
"""

import pandas as pd
import fundamentalanalysis as fa

api_key = 'c99eda5db224d34162adae341298790b'

ticker = "AAPL"

# collect general company information
profile = fa.profile(ticker, api_key)
print(profile)

# collect recent company quotes // techinical data // earnings date
quotes = fa.quote(ticker, api_key)
print(quotes)

# collect the Balance Sheet statements // columns are years
balance_sheet_annually = fa.balance_sheet_statement(
                          ticker, api_key, period="annual")

# individual balance sheet items by year
# print(list(balance_sheet_annually['2021'].index))
print(balance_sheet_annually['2021'])

# collect the Income Statements // columns are years
income_statement_annually = fa.income_statement(
                            ticker, api_key, period="annual")

# individual income statement items by year
print(list(income_statement_annually['2021'].index))
print('- - -')
print(income_statement_annually['2021'].loc['netIncomeRatio'])

# collect the Cash Flow Statements // columns are years
cash_flow_statement_annually = fa.cash_flow_statement(
                                ticker, api_key, period="annual")

# individual income statement items by year
print(list(cash_flow_statement_annually['2021'].index))
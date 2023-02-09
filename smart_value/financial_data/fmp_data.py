# -*- coding: utf-8 -*-
"""
Get fundamental data
@author: Adam Getbags
Data provided by Financial Modeling Prep
https://site.financialmodelingprep.com/developer/docs/
Python API wrapper by
https://github.com/JerBouma/FundamentalAnalysis
"""

#import modules
import pandas as pd
import fundamentalanalysis as fa

api_key = 'c99eda5db224d34162adae341298790b'

ticker = "AAPL"

#collect general company information
profile = fa.profile(ticker, api_key)
display(profile)

#collect recent company quotes // techinical data // earnings date
quotes = fa.quote(ticker, api_key)
print(quotes)
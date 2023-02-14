from datetime import datetime
import xlwings
import pathlib
import re
import smart_value.financial_data.fred_data


us_riskfree = smart_value.financial_data.fred_data.risk_free_rate("us")
cn_riskfree = smart_value.financial_data.fred_data.risk_free_rate("cn")


def update_market(self, pipline_book):
    """Update the Current_Holdings sheet in the Pipeline_monitor file.

    :param pipline_book: xlwings book object
    """

    market_sheet = pipline_book.sheets('Market')

    market_sheet.range('D3').value = self.us_riskfree
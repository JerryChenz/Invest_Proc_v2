import xlwings
import pathlib
import shutil
import os
from datetime import datetime
import re
from smart_value.stock import *
from smart_value.financial_data import yf_data as yf
from smart_value.financial_data import yq_data as yq


def new_stock_model(ticker, source):
    """Creates a new model if it doesn't already exist, then update.

    :param ticker: the string ticker of the stock
    :param source: String data source selector
    :raises FileNotFoundError: raises an exception when there is an error related to the model files or path
    """

    stock_regex = re.compile(".*Stock_Valuation")
    negative_regex = re.compile(".*~.*")

    # Relevant Paths
    cwd = pathlib.Path.cwd().resolve()
    template_folder_path = cwd / 'financial_models' / 'Model_templates' / 'Listed_template'
    new_bool = False

    try:
        # Check if the template exists
        if pathlib.Path(template_folder_path).exists():
            path_list = [val_file_path for val_file_path in template_folder_path.iterdir()
                         if template_folder_path.is_dir() and val_file_path.is_file()]
            template_path_list = list(item for item in path_list if stock_regex.match(str(item)) and
                                      not negative_regex.match(str(item)))
            if len(template_path_list) > 1 or len(template_path_list) == 0:
                raise FileNotFoundError("The template file error", "temp_file")
        else:
            raise FileNotFoundError("The stock_template folder doesn't exist", "temp_folder")
    except FileNotFoundError as err:
        if err.args[1] == "temp_folder":
            print("The stock_template folder doesn't exist")
        if err.args[1] == "temp_file":
            print("The template file error")
    else:
        # New model path
        model_name = ticker + "_" + os.path.basename(template_path_list[0])
        model_path = cwd / 'financial_models' / model_name
        if not pathlib.Path(model_path).exists():
            # Creates a new model file if not already exists in cwd
            print(f'Creating {model_name}...')
            new_bool = True
            shutil.copy(template_path_list[0], model_path)
        # update the model
        update_stock_model(ticker, model_name, model_path, new_bool, source)


def update_stock_model(ticker, model_name, model_path, new_bool, source):
    """Update the model.

    :param ticker: the string ticker of the stock
    :param model_name: the model file name
    :param model_path: the model file path
    :param source: String data source selector
    :param new_bool: False if there is a model exists, true otherwise
    """
    # update the new model
    print(f'Updating {model_name}...')
    company = StockModel(ticker, source)  # uses yahoo finance data by default

    with xlwings.App(visible=False) as app:
        model_xl = app.books.open(model_path)
        update_dashboard(model_xl.sheets('Dashboard'), company, new_bool)
        if new_bool:
            update_data(model_xl.sheets('Data'), model_xl.sheets('Asset_Model'), company)
        model_xl.save(model_path)
        model_xl.close()


def update_dashboard(dash_sheet, stock, new_bool=False):
    """Update the Dashboard sheet.

    :param dash_sheet: the xlwings object of the model
    :param stock: the Stock object
    :param new_bool: False if there is a model exists, true otherwise
    """

    if new_bool:
        dash_sheet.range('C3').value = stock.symbol
        dash_sheet.range('C4').value = stock.name
        dash_sheet.range('C5').value = datetime.today().strftime('%Y-%m-%d')
        dash_sheet.range('I3').value = stock.exchange
        dash_sheet.range('I5').value = stock.shares
        dash_sheet.range('I11').value = stock.report_currency
    dash_sheet.range('I4').value = stock.price[0]
    dash_sheet.range('I12').value = stock.fx_rate


def update_data(data_sheet, asset_sheet, stock):
    """Update the Data sheet.

    :param data_sheet: the xlwings object of Data
    :param asset_sheet: the xlwings object of Asset_Model
    :param stock: the Stock object
    """

    data_sheet.range('C3').value = stock.last_fy
    data_digits = len(str(int(stock.is_df.iloc[0, 0])))
    if data_digits <= 6:
        report_unit = 1
    elif data_digits <= 9:
        report_unit = 1000
    else:
        report_unit = int((data_digits - 9) / 3 + 0.99) * 1000
    data_sheet.range('C4').value = report_unit

    for i in range(len(stock.is_df.columns)):
        # income statement
        data_sheet.range((7, i + 3)).value = int(stock.is_df.iloc[0, i] / report_unit)  # Sales
        data_sheet.range((9, i + 3)).value = int(stock.is_df.iloc[1, i] / report_unit)  # COGS
        data_sheet.range((11, i + 3)).value = int(stock.is_df.iloc[2, i] / report_unit)  # Operating Expenses
        data_sheet.range((18, i + 3)).value = int(stock.is_df.iloc[3, i] / report_unit)  # Interest Expense
        data_sheet.range((19, i + 3)).value = int(stock.is_df.iloc[4, i] / report_unit)  # owner's Net Income

    for j in range(len(stock.annual_bs.columns)):
        # load balance sheet
        data_sheet.range((21, j + 3)).value = int(stock.annual_bs.iloc[1, j] / report_unit)  # CurrentAssets
        data_sheet.range((22, j + 3)).value = int(stock.annual_bs.iloc[2, j] / report_unit)  # CurrentLiabilities
        # ST Interest-bearing Debt = CurrentDebtAndCapitalLeaseObligation
        data_sheet.range((23, j + 3)).value = int(stock.annual_bs.iloc[3, j] / report_unit)
        # CurrentCapitalLeaseObligation
        data_sheet.range((24, j + 3)).value = int(stock.annual_bs.iloc[4, j] / report_unit)
        # LT Interest-bearing Debt = LongTermDebtAndCapitalLeaseObligation
        data_sheet.range((25, j + 3)).value = int(stock.annual_bs.iloc[5, j] / report_unit)
        # LongTermCapitalLeaseObligation
        data_sheet.range((26, j + 3)).value = int(stock.annual_bs.iloc[6, j] / report_unit)
        # TotalEquityAndMinorityInterest
        data_sheet.range((28, j + 3)).value = int(stock.annual_bs.iloc[7, j] / report_unit)
        data_sheet.range((29, j + 3)).value = int(stock.annual_bs.iloc[8, j] / report_unit)  # CommonStockEquity
        data_sheet.range((30, j + 3)).value = int(stock.annual_bs.iloc[14, j] / report_unit)  # NetPPE

    for k in range(len(stock.cf_df.columns)):
        # Cash flow statement
        data_sheet.range((31, k + 3)).value = int(stock.cf_df.iloc[5, k] / report_unit)  # EndCashPosition
        data_sheet.range((37, k + 3)).value = int(-stock.cf_df.iloc[3, k] / report_unit)  # CashDividendsPaid
        data_sheet.range((38, k + 3)).value = int(-stock.cf_df.iloc[4, k] / report_unit)  # RepurchaseOfCapitalStock

    update_asset(asset_sheet, stock, report_unit)


def update_asset(dash_sheet, stock, report_unit):
    """Update the Dashboard sheet.

    :param dash_sheet: the xlwings object of the model
    :param report_unit: financial statement unit
    :param stock: the Stock object
    """

    dash_sheet.range('D3').value = stock.quarter_bs.iloc[7, 0] / report_unit  # total equity
    dash_sheet.range('I3').value = stock.quarter_bs.iloc[8, 0] / report_unit  # common equity
    dash_sheet.range('D9').value = stock.most_recent_quarter  # result date
    dash_sheet.range('I26').value = stock.quarter_bs.iloc[2, 0] / report_unit  # current liabilities
    # Non-current liabilities = Total Asset - Total Equity - Current Liabilities
    dash_sheet.range('I42').value = (stock.quarter_bs.iloc[0, 0] - stock.quarter_bs.iloc[7, 0] -
                                     stock.quarter_bs.iloc[2, 0]) / report_unit


# update dash only, not touching the data tab
def update_dash(ticker, source):
    """Update the dashboard of the model.

    :param ticker: the string ticker of the stock
    :param source: String data source selector
    :raises FileNotFoundError: raises an exception when there is an error related to the model files or path
    """

    # Relevant Paths
    opportunities_folder_path = pathlib.Path.cwd().resolve() / 'financial_models' / 'Opportunities'
    path_list = []

    if pathlib.Path(opportunities_folder_path).exists():
        path_list = [val_file_path for val_file_path in opportunities_folder_path.iterdir()
                     if opportunities_folder_path.is_dir() and val_file_path.is_file()]
    for p in path_list:
        if ticker in p.stem:
            with xlwings.App(visible=False) as app:
                xl_book = app.books.open(p)
                dash_sheet = xl_book.sheets('Dashboard')
                company = StockModel(ticker, source)
                update_dashboard(dash_sheet, company)
                xl_book.save(p)
                xl_book.close()


class StockModel(Stock):
    """Stock model class"""

    def __init__(self, symbol, source):
        """
        :param symbol: string ticker of the stock
        :param source: data source selector
        """
        super().__init__(symbol)
        self.source = source
        self.load_attributes()

    def load_attributes(self):
        """data source selector."""
        try:
            if self.source == "yf":
                ticker_data = yf.YfData(self.symbol)
                self.load_data(ticker_data)
                self.fx_rate = yf.get_forex(self.report_currency, self.price[1])

            elif self.source == "yf_quote":
                market_price = yf.get_quote(self.symbol, "last_price")
                price_currency = yf.get_quote(self.symbol, "currency")
                report_currency = yf.get_info(self.symbol, "financialCurrency")
                self.load_quote(market_price, price_currency, report_currency)
                self.fx_rate = yf.get_forex(report_currency, price_currency)

            elif self.source == "yq":
                ticker_data = yq.YqData(self.symbol)
                self.load_data(ticker_data)
                self.fx_rate = yq.get_forex(self.report_currency, self.price[1])

            elif self.source == "yq_quote":
                quote = yq.get_quote(self.symbol)
                market_price = quote[0]
                price_currency = quote[1]
                report_currency = quote[2]
                self.load_quote(market_price, price_currency, report_currency)
                self.fx_rate = yq.get_forex(report_currency, price_currency)

            elif self.source == "fmp":
                pass

            else:
                raise KeyError(f"The source keyword {self.source} is invalid!")
        except KeyError as error:
            print('Caught this error: ' + repr(error))

    def load_data(self, ticker_data):
        """Scrap the financial_data from yahoo finance

        :param ticker_data: financial data object
        """

        self.name = ticker_data.name
        self.sector = ticker_data.sector
        self.price = ticker_data.price
        self.exchange = ticker_data.exchange
        self.shares = ticker_data.shares
        self.report_currency = ticker_data.report_currency
        self.last_dividend = ticker_data.last_dividend
        self.buyback = ticker_data.buyback
        self.annual_bs = ticker_data.annual_bs
        self.quarter_bs = ticker_data.quarter_bs
        self.cf_df = ticker_data.cf_df
        self.is_df = ticker_data.is_df
        self.last_fy = ticker_data.annual_bs.columns[0]
        self.most_recent_quarter = ticker_data.most_recent_quarter

    def load_quote(self, market_price, price_currency, report_currency):
        """Get the quick market quote

        :param market_price: current or delayed market price
        :param price_currency: currency symbol of the market_price
        :param report_currency: currency symbol of the financial statements
        """

        self.price = [market_price, price_currency]
        self.fx_rate = yf.get_forex(report_currency, price_currency)

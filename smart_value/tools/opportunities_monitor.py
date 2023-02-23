from datetime import datetime
import xlwings
import pathlib
import re
import smart_value.stock
import smart_value.tools.stock_model
import smart_value.financial_data.fred_data


def get_opportunties_paths():
    """Load the asset information from the opportunities folder

    return a list of paths pointing to the models
    """

    # Copy the latest Valuation template
    opportunities_folder_path = pathlib.Path.cwd().resolve() / 'financial_models' / 'Opportunities'
    r = re.compile(".*Valuation")

    try:
        if pathlib.Path(opportunities_folder_path).exists():
            path_list = [val_file_path for val_file_path in opportunities_folder_path.iterdir()
                         if opportunities_folder_path.is_dir() and val_file_path.is_file()]
            opportunities_path_list = list(item for item in path_list if r.match(str(item)))
            if len(opportunities_path_list) == 0:
                raise FileNotFoundError("No opportunity file", "opp_file")
        else:
            raise FileNotFoundError("The opportunities folder doesn't exist", "opp_folder")
    except FileNotFoundError as err:
        if err.args[1] == "opp_folder":
            print("The opportunities folder doesn't exist")
        if err.args[1] == "opp_file":
            print("No opportunity file", "opp_file")
    else:
        return opportunities_path_list


def read_stock(dash_sheet):
    """Read the opportunity from the models in the opportunities' folder.

    :param dash_sheet: xlwings object of the model
    :return: a Stock object
    """

    company = smart_value.stock.Stock(dash_sheet.range('C3').value)
    company.name = dash_sheet.range('C4').value
    company.exchange = dash_sheet.range('I3').value
    company.price = dash_sheet.range('I4').value
    company.price_currency = dash_sheet.range('J4').value
    company.excess_return = dash_sheet.range('I16').value
    company.fcf_value = dash_sheet.range('B17').value
    company.ideal_price = dash_sheet.range('B18').value
    company.navps = dash_sheet.range('G14').value
    company.realizable_value = dash_sheet.range('D14').value
    company.nonop_assets = dash_sheet.range('F21').value
    company.dividend = dash_sheet.range('C7').value
    company.last_result = dash_sheet.range('C6').value
    return company


def update_monitor(monitor_file_path, op_list):
    """Update the Monitor file

    :param op_list: list of stock objects
    :param monitor_file_path: the path of the Monitor file
    """

    with xlwings.App(visible=False) as app:
        pipline_book = app.books.open(monitor_file_path)
        update_opportunities(pipline_book, op_list)
        # update_holdings(pipline_book, op_list)
        pipline_book.save(monitor_file_path)
        pipline_book.close()


def update_opportunities(pipline_book, op_list):
    """Update the opportunities sheet in the Pipeline_monitor file

    :param op_list: list of stock objects
    :param pipline_book: xlwings book object
    """

    monitor_sheet = pipline_book.sheets('Opportunities')
    monitor_sheet.range('B5:N200').clear_contents()

    r = 5
    for a in op_list:
        monitor_sheet.range((r, 2)).value = a.symbol
        monitor_sheet.range((r, 3)).value = a.name
        monitor_sheet.range((r, 4)).value = a.sector
        monitor_sheet.range((r, 5)).value = a.exchange
        monitor_sheet.range((r, 6)).value = a.price
        monitor_sheet.range((r, 7)).value = a.price_currency
        monitor_sheet.range((r, 8)).value = f'=F{r}-I{r}'
        monitor_sheet.range((r, 9)).value = a.nonop_assets
        monitor_sheet.range((r, 10)).value = a.excess_return
        monitor_sheet.range((r, 11)).value = a.ideal_price
        monitor_sheet.range((r, 12)).value = a.fcf_value
        monitor_sheet.range((r, 13)).value = a.realizable_value
        monitor_sheet.range((r, 14)).value = a.navps
        monitor_sheet.range((r, 15)).value = a.dividend
        monitor_sheet.range((r, 16)).value = a.last_result
        r += 1


def update_holdings(pipline_book, op_list):
    """Update the Current_Holdings sheet in the Pipeline_monitor file.

    :param op_list: list of stock objects
    :param pipline_book: xlwings book object
    """

    holding_sheet = pipline_book.sheets('Current_Holdings')
    holding_sheet.range('B7:O200').clear_contents()

    k = 7
    for a in op_list:
        if a.total_units:
            holding_sheet.range((k, 2)).value = a.symbol
            holding_sheet.range((k, 3)).value = a.name
            holding_sheet.range((k, 4)).value = a.exchange
            holding_sheet.range((k, 5)).value = a.price_currency
            holding_sheet.range((k, 6)).value = a.unit_cost
            holding_sheet.range((k, 7)).value = a.total_units
            holding_sheet.range((k, 8)).value = f'=F{k}*G{k}'
            # holding_sheet.range((k, 9)).value =
            # holding_sheet.range((k, 10)).value =
            k += 1

    # Current Holdings
    holding_sheet.range('I2').value = datetime.today().strftime('%Y-%m-%d')


class MonitorStock:
    """Monitor class"""
    opportunities = []

    def __init__(self):
        self.symbol = None
        self.name = None
        self.exchange = None
        self.price = None
        self.price_currency = None
        self.price_range = None
        self.excess_return = None
        self.fcf_value = None
        self.target_price_1 = None
        self.target_price_2 = None
        self.realizable_value = None
        self.nonop_assets = None
        self.frd_dividend = None
        self.last_result = None
        self.next_review = None
        # load and update the new valuation xlsx
        for opportunities_path in get_opportunties_paths():
            # load and update the new valuation xlsx
            print(f"Working with {opportunities_path}...")
            MonitorStock.opportunities.append(self.read_opportunity(opportunities_path))

    def load_data(self):
        """"""


        # load the opportunities
        monitor_file_path = opportunities_folder_path / 'Monitor' / 'Monitor.xlsx'
        print("Updating Monitor...")
        update_monitor(monitor_file_path, opportunities)

    def read_opportunity(self, opportunities_path):
        """Read all the opportunities at the opportunities_path.

        :param opportunities_path: path of the model in the opportunities' folder
        :return: an Asset object
        """

        opportunity = None
        r_stock = re.compile(".*_Stock_Valuation")
        # get the formula results using xlwings because openpyxl doesn't evaluate formula
        with xlwings.App(visible=False) as app:
            xl_book = app.books.open(opportunities_path)
            dash_sheet = xl_book.sheets('Dashboard')

            if r_stock.match(str(opportunities_path)):
                # Update the models first in the opportunities folder
                company = smart_value.tools.stock_model.StockModel(dash_sheet.range('C3').value, "yf_quote")
                smart_value.tools.stock_model.update_dashboard(dash_sheet, company)
                xl_book.save(opportunities_path)  # xls must be saved to update the values
                opportunity = read_stock(dash_sheet)
            else:
                pass  # to be implemented
            xl_book.close()
        return opportunity

    def load_attributes(self, dash_sheet):
        self.name = dash_sheet.range('C4').value
        self.exchange = dash_sheet.range('I3').value
        self.price = dash_sheet.range('I4').value
        self.price_currency = dash_sheet.range('J4').value
        self.price_range = dash_sheet.range('G14').value
        self.excess_return = dash_sheet.range('I16').value
        self.fcf_value = dash_sheet.range('B17').value
        self.target_price_1 = dash_sheet.range('B18').value
        self.target_price_2 = dash_sheet.range('B18').value
        self.realizable_value = dash_sheet.range('D14').value
        self.nonop_assets = dash_sheet.range('F21').value
        self.frd_dividend = dash_sheet.range('C7').value
        self.last_result = dash_sheet.range('C6').value
        self.next_review = None

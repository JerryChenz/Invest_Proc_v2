from datetime import datetime
import xlwings
import pathlib
import re
import smart_value.tools.stock_model
from smart_value.financial_data.fred_data import risk_free_rate, inflation
from smart_value.financial_data.hkma_data import get_hk_riskfree

models_folder_path = pathlib.Path.cwd().resolve() / 'financial_models' / 'Opportunities'
p_monitor_file_path = models_folder_path / 'Monitor' / 'Portfolio_Monitor.xlsx'
m_monitor_file_path = models_folder_path / 'Monitor' / 'Macro_Monitor.xlsx'


def update_monitor():
    """Update the Monitor file"""

    opportunities = []

    update_marco(m_monitor_file_path, "Free")  # Update the marco Monitor

    # load and update the new valuation xlsx
    for opportunities_path in get_model_paths():
        print(f"Working with {opportunities_path}...")
        op = read_opportunity(opportunities_path)  # load and update the new valuation xlsx
        opportunities.append(op)

    print("Updating Monitor...")
    with xlwings.App(visible=False) as app:
        pipline_book = app.books.open(p_monitor_file_path)
        update_opportunities(pipline_book, opportunities)
        update_holdings(pipline_book, opportunities)
        pipline_book.save(p_monitor_file_path)
        pipline_book.close()
    print("Update completed")


def update_marco(monitor_path, source):
    """Update the Current_Holdings sheet in the Pipeline_monitor file.

    :param source: the API option
    :param monitor_path: path fo the Monitor excel
    """

    print("Updating Marco data...")
    us_riskfree = 0.08
    cn_riskfree = 0.06
    hk_riskfree = us_riskfree
    us_inflation = 0.02

    if source == "Free":
        us_riskfree = risk_free_rate("us")
        # print(us_riskfree)
        cn_riskfree = risk_free_rate("cn")
        # print(cn_riskfree)
        hk_riskfree = get_hk_riskfree()
        # print(hk_riskfree)
        us_inflation = inflation("us")

    with xlwings.App(visible=False) as app:
        marco_book = app.books.open(monitor_path)
        macro_sheet = marco_book.sheets('Macro')
        macro_sheet.range('D6').value = us_riskfree
        macro_sheet.range('F6').value = cn_riskfree
        macro_sheet.range('H6').value = hk_riskfree
        macro_sheet.range('D7').value = us_inflation
        marco_book.save(monitor_path)
        marco_book.close()
    print("Finished Marco data Update")


def read_opportunity(opportunities_path):
    """Read all the opportunities at the opportunities_path.

    :param opportunities_path: path of the model in the opportunities' folder
    :return: an Asset object
    """

    r_stock = re.compile(".*_Stock_Valuation")
    # get the formula results using xlwings because openpyxl doesn't evaluate formula
    with xlwings.App(visible=False) as app:
        xl_book = app.books.open(opportunities_path)
        dash_sheet = xl_book.sheets('Dashboard')
        asset_sheet = xl_book.sheets('Asset_Model')
        if r_stock.match(str(opportunities_path)):
            company = smart_value.tools.stock_model.StockModel(dash_sheet.range('C3').value, "yq_quote")
            smart_value.tools.stock_model.update_dashboard(dash_sheet, company)  # Update
            xl_book.save(opportunities_path)  # xls must be saved to update the values
            op = MonitorStock(dash_sheet, asset_sheet)  # the MonitorStock object representing an opportunity
        else:
            print(f"'{opportunities_path}' is incorrect")
        xl_book.close()

    return op


def get_model_paths():
    """Load the asset information from the opportunities folder

    return a list of paths pointing to the models
    """

    # Copy the latest Valuation template
    r = re.compile(".*Valuation")

    try:
        if pathlib.Path(models_folder_path).exists():
            path_list = [val_file_path for val_file_path in models_folder_path.iterdir()
                         if models_folder_path.is_dir() and val_file_path.is_file()]
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


def update_opportunities(pipline_book, op_list):
    """Update the opportunities sheet in the Pipeline_monitor file

    :param op_list: list of stock objects
    :param pipline_book: xlwings book object
    """

    monitor_sheet = pipline_book.sheets('Opportunities')
    monitor_sheet.range('B5:S400').clear_contents()

    r = 5
    for op in op_list:
        monitor_sheet.range((r, 2)).value = op.symbol
        monitor_sheet.range((r, 3)).value = op.name
        monitor_sheet.range((r, 4)).value = op.price
        monitor_sheet.range((r, 5)).value = op.price_currency
        monitor_sheet.range((r, 6)).value = op.current_excess_return
        monitor_sheet.range((r, 7)).value = op.frd_dividend
        monitor_sheet.range((r, 8)).value = op.next_buy_price
        monitor_sheet.range((r, 9)).value = op.next_buy_shares
        monitor_sheet.range((r, 10)).value = op.next_sell_price
        monitor_sheet.range((r, 11)).value = op.val_floor
        monitor_sheet.range((r, 12)).value = op.val_ceil
        monitor_sheet.range((r, 13)).value = op.fcf_value
        monitor_sheet.range((r, 14)).value = op.breakeven_price
        monitor_sheet.range((r, 15)).value = op.ideal_price
        monitor_sheet.range((r, 16)).value = op.lfy_date
        monitor_sheet.range((r, 17)).value = op.next_review
        monitor_sheet.range((r, 18)).value = op.exchange
        monitor_sheet.range((r, 19)).value = op.inv_category
        r += 1
    print(f"Total {len(op_list)} opportunities Updated")


def update_holdings(pipline_book, op_list):
    """Update the Current_Holdings sheet in the Pipeline_monitor file.

    :param op_list: list of stock objects
    :param pipline_book: xlwings book object
    """

    holding_sheet = pipline_book.sheets('Current_Holdings')
    holding_sheet.range('B8:R200').clear_contents()

    k = 8
    for op in op_list:
        if op.hold:
            holding_sheet.range((k, 2)).value = op.symbol
            holding_sheet.range((k, 3)).value = op.name
            holding_sheet.range((k, 4)).value = op.price
            holding_sheet.range((k, 5)).value = op.price_hold
            holding_sheet.range((k, 6)).value = op.price_currency
            holding_sheet.range((k, 7)).value = op.shares_hold
            holding_sheet.range((k, 8)).value = op.next_sell_price
            holding_sheet.range((k, 9)).value = op.debt_ce
            holding_sheet.range((k, 10)).value = op.book_quick
            holding_sheet.range((k, 11)).value = op.st_cash_debt
            holding_sheet.range((k, 12)).value = op.st_quick
            holding_sheet.range((k, 13)).value = op.lt_cash_debt
            holding_sheet.range((k, 14)).value = op.lt_quick
            # holding_sheet.range((k, 15)).value = op.
            # holding_sheet.range((k, 16)).value = op.
            holding_sheet.range((k, 17)).value = op.exchange
            holding_sheet.range((k, 18)).value = op.inv_category
            k += 1

    # Current Holdings
    holding_sheet.range('F2').value = datetime.today().strftime('%Y-%m-%d')
    print("Current_holdings updated")


class MonitorStock:
    """Monitor class"""

    def __init__(self, dash_sheet, asset_sheet):
        self.symbol = dash_sheet.range('C3').value
        self.name = dash_sheet.range('C4').value
        self.exchange = dash_sheet.range('I3').value
        self.inv_category = dash_sheet.range('D20').value
        self.price = dash_sheet.range('I4').value
        self.price_currency = dash_sheet.range('J4').value
        self.current_excess_return = dash_sheet.range('D16').value
        self.val_floor = dash_sheet.range('D14').value
        self.val_ceil = dash_sheet.range('F14').value
        self.fcf_value = dash_sheet.range('H14').value
        self.breakeven_price = dash_sheet.range('B17').value
        self.next_buy_price = dash_sheet.range('C35').value
        self.next_buy_shares = dash_sheet.range('C36').value
        self.next_sell_price = dash_sheet.range('I35').value
        self.ideal_price = dash_sheet.range('J25').value
        self.lfy_date = dash_sheet.range('E6').value  # date of the last financial year-end
        self.next_review = dash_sheet.range('D6').value
        self.frd_dividend = dash_sheet.range('F16').value
        # Current holdings attributes
        self.hold = dash_sheet.range('C26').value != ""
        self.shares_hold = dash_sheet.range('C28').value
        self.price_hold = dash_sheet.range('C29').value
        self.debt_ce = asset_sheet.range('I53').value
        self.book_quick = asset_sheet.range('I7').value
        self.st_cash_debt = asset_sheet.range('I24').value
        self.st_quick = asset_sheet.range('I25').value
        self.lt_cash_debt = asset_sheet.range('I44').value
        self.lt_quick = asset_sheet.range('I45').value

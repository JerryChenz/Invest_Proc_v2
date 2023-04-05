from datetime import datetime
import smart_value


def gen_val_xlsx(ticker, source):
    """generate or update a valuation file with argument, ticker

    :param ticker: string ticker symbol
    :param source: String data source selector - "yf", "yq"
    """

    smart_value.tools.stock_model.new_stock_model(ticker, source)


def update_val_xlsx(ticker, source):
    """Update dashboard only, not touching the data tab
    
    :param ticker: string ticker symbol
    :param source: String data source selector - "yf", "yq"
    """

    smart_value.tools.stock_model.update_dash(ticker, source)


def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return abs((d2 - d1).days)


if __name__ == '__main__':
    stare_list = ['1122.HK']
    for s in stare_list:
        gen_val_xlsx(s, "yq")
        # update_val_xlsx(s, "yq")

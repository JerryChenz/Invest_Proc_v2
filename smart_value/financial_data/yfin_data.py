import yahoo_fin.stock_info as yfin



def market_info(ticker_code):
    """
    live price

    @:param ticker_code: string
    @:return: [numpy.float64, string, string]
    Return the live price of the stock"""

    price = yfin.get_live_price(ticker_code)
    price_currency = yfin.get_quote_data(ticker_code)['currency']
    report_currency =  yfin.get_quote_data(ticker_code)['financialCurrency']

    return [price, price_currency, report_currency]
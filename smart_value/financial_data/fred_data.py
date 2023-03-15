from fredapi import Fred


fred_api_key = '25dcdb108d7d62628268b97f9df6b593'


def risk_free_rate(country):
    """Return the 10-year government bond yield
    :param country: us or cn
    :return: 10-year government bond yield"""

    fred = Fred(api_key=fred_api_key)

    if country == 'cn':
        return fred.get_series('INTDSRCNM193N')  # China Discount Rate
    else:
        return fred.get_series('DGS10')  # US 10 Year Treasury Yield

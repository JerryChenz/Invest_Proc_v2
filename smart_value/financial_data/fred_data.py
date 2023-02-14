from fredapi import Fred


fred_api_key = '25dcdb108d7d62628268b97f9df6b593'


def risk_free_rate(country):
    """Return the 10-year government bond yield
    :param country: us or cn
    :return: 10-year government bond yield"""

    if country == 'cn':
        pass
        # Todo: China 10 Year Treasury Yield
    else:
        fred = Fred(api_key=fred_api_key)
        return fred.get_series('DGS10')  # US 10 Year Treasury Yield

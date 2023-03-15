import urllib.request
import json


def get_hk_riskfree():
    treasury_url = 'https://api.hkma.gov.hk/public/market-data-and-statistics/monthly-statistical-bulletin/gov-bond' \
                   '/instit-bond-price-yield-daily?segment=Benchmark&offset=0'

    # output rate not in percentage
    with urllib.request.urlopen(treasury_url) as req:
        byte_str = req.read()
        dict_str = byte_str.decode("UTF-8")
        data_dict = json.loads(dict_str)
        return data_dict['result']['records'][0]['ind_pricing_10y']/100


class Asset:
    """Parent class"""

    def __init__(self, security_code):
        self.symbol = security_code
        self.name = None
        self.exchange = None
        self.price = None
        self.price_currency = None
        self.shares = None
        self.excess_return = None
        self.ideal_price = None
        self.fcf_value = None
        self.navps = None
        self.realizable_value = None
        self.nonop_assets = None
        self.periodic_payment = None  # dividend for stocks and coupon for bonds
        self.last_fy = None  # last coupon date for bonds
        self.last_result = None # may not be the same as last_fy
        self.total_units = None
        self.unit_cost = None
        self.is_updated = None


"""More updates planned for more powerful Monitor features using pandas"""
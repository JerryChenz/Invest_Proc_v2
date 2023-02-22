
class Asset:
    """Parent class"""

    def __init__(self, symbol):
        self.symbol = symbol
        self.name = None
        self.exchange = None
        self.price = None
        self.price_currency = None

from smart_value.asset import *


class Bond(Asset):
    """a type of Assets"""

    def __init__(self, symbol, source):
        """
        :param symbol: string ticker of the stock
        :param source: data source selector
        """
        super().__init__(symbol)

        self.total_units = None
        self.unit_cost = None
        self.ytm = None
        self.source = source

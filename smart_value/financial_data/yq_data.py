import pandas as pd
from datetime import datetime
from yahooquery import Ticker


class YqData:
    """Retrieves the data from YH Finance API and yfinance package"""

    def __init__(self, symbol):
        """
        :param symbol: string symbol of the stock
        """
        self.symbol = symbol
        try:
            self.stock_data = Ticker(self.symbol)
        except KeyError:
            print("Check your stock ticker")
        self.name = self.stock_data.quote_type['shortName']
        self.sector = self.stock_data.asset_profile['sector']
        self.price = [self.stock_data.financial_data['currentPrice'], self.stock_data.price['currency']]
        self.exchange = self.stock_data.quote_type['exchange']
        self.shares = self.stock_data.key_stats['sharesOutstanding']
        self.report_currency = self.stock_data.financial_data['financialCurrency']
        self.annual_bs = self.get_balance_sheet("annual")
        self.quarter_bs = self.get_balance_sheet("quarterly")
        self.income_statement = self.get_income_statement()
        self.cash_flow = self.get_cash_flow()
        # left most column contains the most recent data
        self.most_recent_quarter = pd.to_datetime(datetime.fromtimestamp(self.quarter_bs.columns.values.tolist()[0])
                                                  .strftime("%Y-%m-%d"))
        try:
            self.dividends = -int(self.cash_flow.loc['CashDividendsPaid'][0]) / self.shares
        except ZeroDivisionError:
            self.dividends = 0
        try:
            self.buyback = -int(self.cash_flow.loc['RepurchaseOfCapitalStock'][0]) / self.shares
        except ZeroDivisionError:
            self.buyback = 0

    def get_balance_sheet(self, option="annual"):
        """Returns a DataFrame with selected balance sheet data

        :param option: annual or quarterly

        balance sheet keys:
          ['asOfDate', 'periodType', 'currencyCode', 'AccountsPayable',
               'AccountsReceivable', 'AccumulatedDepreciation',
               'AllowanceForDoubtfulAccountsReceivable', 'AvailableForSaleSecurities',
               'BuildingsAndImprovements', 'CapitalLeaseObligations', 'CapitalStock',
               'CashAndCashEquivalents', 'CashCashEquivalentsAndShortTermInvestments',
               'CashFinancial', 'CommonStock', 'CommonStockEquity',
               'ConstructionInProgress', 'CurrentAssets',
               'CurrentCapitalLeaseObligation', 'CurrentDebtAndCapitalLeaseObligation',
               'CurrentLiabilities', 'DividendsPayable',
               'FinancialAssetsDesignatedasFairValueThroughProfitorLossTotal',
               'FinishedGoods', 'Goodwill', 'GoodwillAndOtherIntangibleAssets',
               'GrossAccountsReceivable', 'GrossPPE', 'Inventory', 'InvestedCapital',
               'InvestmentinFinancialAssets', 'InvestmentsinAssociatesatCost',
               'LandAndImprovements', 'LongTermCapitalLeaseObligation',
               'LongTermDebtAndCapitalLeaseObligation', 'LongTermEquityInvestment',
               'MachineryFurnitureEquipment', 'MinorityInterest', 'NetPPE',
               'NetTangibleAssets', 'NonCurrentDeferredRevenue',
               'NonCurrentDeferredTaxesAssets', 'NonCurrentDeferredTaxesLiabilities',
               'NonCurrentPrepaidAssets', 'OrdinarySharesNumber',
               'OtherEquityInterest', 'OtherIntangibleAssets', 'OtherInvestments',
               'OtherPayable', 'OtherProperties', 'OtherReceivables',
               'OtherShortTermInvestments', 'Payables', 'PrepaidAssets', 'Properties',
               'RawMaterials', 'RetainedEarnings', 'ShareIssued', 'StockholdersEquity',
               'TangibleBookValue', 'TaxesReceivable', 'TotalAssets',
               'TotalCapitalization', 'TotalDebt', 'TotalEquityGrossMinorityInterest',
               'TotalLiabilitiesNetMinorityInterest', 'TotalNonCurrentAssets',
               'TotalNonCurrentLiabilitiesNetMinorityInterest', 'TotalTaxPayable',
               'TreasuryStock', 'WorkInProcess', 'WorkingCapital']
        """

        dummy = {
            "Dummy": [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None]}

        bs_index = ['TotalAssets', 'CurrentAssets', 'CurrentLiabilities', 'CurrentDebtAndCapitalLeaseObligation',
                    'CurrentCapitalLeaseObligation', 'LongTermDebtAndCapitalLeaseObligation',
                    'LongTermCapitalLeaseObligation', 'TotalEquityGrossMinorityInterest', 'MinorityInterest',
                    'CashAndCashEquivalents', 'OtherShortTermInvestments', 'InvestmentProperties',
                    'LongTermEquityInvestment', 'InvestmentinFinancialAssets', 'NetPPE']

        if option == "annual":
            # reverses the dataframe with .iloc[:, ::-1]
            balance_sheet = self.stock_data.balance_sheet(trailing=False).T.iloc[:, ::-1]
        else:
            balance_sheet = self.stock_data.balance_sheet(frequency="q", trailing=False).T.iloc[:, ::-1]
        # Start of Cleaning: make sure the data has all the required indexes
        dummy_df = pd.DataFrame(dummy, index=bs_index)
        clean_bs = dummy_df.join(balance_sheet)
        bs_df = clean_bs.loc[bs_index]
        # Ending of Cleaning: drop the dummy column after join
        bs_df.drop('Dummy', inplace=True, axis=1)

        return bs_df.fillna(0)

    def get_income_statement(self):
        """Returns a DataFrame with selected income statement data

        income statement keys:
        ['asOfDate', 'periodType', 'currencyCode', 'BasicAverageShares',
       'BasicEPS', 'CostOfRevenue', 'DilutedAverageShares', 'DilutedEPS',
       'DilutedNIAvailtoComStockholders', 'EBIT',
       'GeneralAndAdministrativeExpense', 'GrossProfit',
       'ImpairmentOfCapitalAssets', 'InterestExpense',
       'InterestExpenseNonOperating', 'InterestIncome',
       'InterestIncomeNonOperating', 'MinorityInterests', 'NetIncome',
       'NetIncomeCommonStockholders', 'NetIncomeContinuousOperations',
       'NetIncomeFromContinuingAndDiscontinuedOperation',
       'NetIncomeFromContinuingOperationNetMinorityInterest',
       'NetIncomeIncludingNoncontrollingInterests', 'NetInterestIncome',
       'NetNonOperatingInterestIncomeExpense', 'NormalizedEBITDA',
       'NormalizedIncome', 'OperatingExpense', 'OperatingIncome',
       'OperatingRevenue', 'OtherNonOperatingIncomeExpenses',
       'OtherSpecialCharges', 'OtherunderPreferredStockDividend',
       'PretaxIncome', 'ReconciledCostOfRevenue', 'ReconciledDepreciation',
       'SellingAndMarketingExpense', 'SellingGeneralAndAdministration',
       'SpecialIncomeCharges', 'TaxEffectOfUnusualItems', 'TaxProvision',
       'TaxRateForCalcs', 'TotalExpenses', 'TotalRevenue', 'TotalUnusualItems',
       'TotalUnusualItemsExcludingGoodwill', 'WriteOff']
        """

        # reverses the dataframe with .iloc[:, ::-1]
        income_statement = self.stock_data.income_statement(trailing=False).set_index('asOfDate').T.iloc[:, ::-1]
        print(income_statement.index)
        # Start of Cleaning: make sure the data has all the required indexes
        dummy = {"Dummy": [None, None, None, None, None]}
        is_index = ['TotalRevenue', 'CostOfRevenue', 'SellingGeneralAndAdministration', 'InterestExpense',
                    'NetIncomeCommonStockholders']
        dummy_df = pd.DataFrame(dummy, index=is_index)
        clean_is = dummy_df.join(income_statement)
        is_df = clean_is.loc[is_index]
        # Ending of Cleaning: drop the dummy column after join
        is_df.drop('Dummy', inplace=True, axis=1)
        is_df = is_df.fillna(0)

        return is_df

    def get_cash_flow(self):
        """Returns a DataFrame with selected Cash flow statement data

        cash flow statement keys:
        ['periodType', 'currencyCode', 'AmortizationCashFlow',
       'BeginningCashPosition', 'CapitalExpenditure', 'CashDividendsPaid',
       'ChangeInCashSupplementalAsReported', 'ChangeInInventory',
       'ChangeInOtherCurrentAssets', 'ChangeInPayable', 'ChangeInReceivables',
       'ChangeInWorkingCapital', 'ChangesInCash', 'CommonStockDividendPaid',
       'CommonStockIssuance', 'CommonStockPayments', 'Depreciation',
       'DepreciationAndAmortization', 'EffectOfExchangeRateChanges',
       'EndCashPosition', 'FinancingCashFlow', 'FreeCashFlow',
       'GainLossOnInvestmentSecurities', 'GainLossOnSaleOfPPE',
       'InterestPaidCFF', 'InterestReceivedCFI', 'InvestingCashFlow',
       'IssuanceOfCapitalStock', 'LongTermDebtPayments',
       'NetBusinessPurchaseAndSale', 'NetCommonStockIssuance', 'NetIncome',
       'NetIncomeFromContinuingOperations', 'NetInvestmentPurchaseAndSale',
       'NetIssuancePaymentsOfDebt', 'NetLongTermDebtIssuance',
       'NetOtherFinancingCharges', 'NetOtherInvestingChanges',
       'NetPPEPurchaseAndSale', 'OperatingCashFlow', 'OtherNonCashItems',
       'PurchaseOfBusiness', 'PurchaseOfInvestment', 'PurchaseOfPPE',
       'RepaymentOfDebt', 'RepurchaseOfCapitalStock', 'SaleOfInvestment',
       'SaleOfPPE', 'StockBasedCompensation', 'TaxesRefundPaid']
        """

        # reverses the dataframe with .iloc[:, ::-1]
        cash_flow = self.stock_data.cash_flow(trailing=False).set_index('asOfDate').T.iloc[:, ::-1]
        # Start of Cleaning: make sure the data has all the required indexes
        dummy = {"Dummy": [None, None, None, None, None]}
        cf_index = ['OperatingCashFlow', 'InvestingCashFlow', 'FinancingCashFlow',
                    'CashDividendsPaid', 'RepurchaseOfCapitalStock']
        dummy_df = pd.DataFrame(dummy, index=cf_index)
        clean_cf = dummy_df.join(cash_flow)
        cf_df = clean_cf.loc[cf_index]
        # Ending of Cleaning: drop the dummy column after join
        cf_df.drop('Dummy', inplace=True, axis=1)

        return cf_df.fillna(0)

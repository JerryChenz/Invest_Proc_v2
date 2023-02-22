from smart_value.stock import *
from smart_value.tools.stock_screener import *
from yahooquery import Ticker
from forex_python.converter import CurrencyRates
import time


def download_yq(symbols, attempt, failure_list):
    """
        Download the stock data and export them into 4 json files:
        1. intro_data, 2. bs_data, 3. is_data. 4. cf_data

        :param attempt: try_count
        :param symbols: list of symbols separated by a space
        :param failure_list: Tracking the failing symbols
        :return updated failure list
        """

    # external API error re-try
    max_try = 2

    info_col = ['shortName', 'sector', 'industry', 'market', 'sharesOutstanding', 'financialCurrency',
                'lastFiscalYearEnd', 'mostRecentQuarter']
    yf_companies = Ticker(symbols)
    symbol_list = symbols.split(" ")
    while symbol_list:
        symbol = symbol_list.pop(0)  # pop from the beginning
        try:
            # introductory information
            info = pd.Series(yf_companies.tickers[symbol].info)
            # info['currency'] = companies.tickers[symbol].fast_info['currency']  # get it when updating price
            # info['exchange'] = companies.tickers[symbol].fast_info['exchange']  # Use market instead
            info = info.loc[info_col]
            # Balance Sheet
            bs_df = yf_companies.tickers[symbol].quarterly_balance_sheet
            bs_df = format_data(bs_df)

            # Income statement
            is_df = yf_companies.tickers[symbol].financials
            is_df = format_data(is_df)
            # # Cash Flow statement
            cf_df = yf_companies.tickers[symbol].cashflow
            cf_df = format_data(cf_df)
            # Create one big one-row stock_df by using side-by-side merge
            stock_df = pd.concat([info, bs_df, is_df, cf_df])
            stock_df = stock_df.transpose()
            stock_df['ticker'] = symbol
            stock_df.set_index('ticker').to_json(json_dir / f"{symbol}_data.json")
            if attempt == 0:
                print(f"{symbol} exported, {len(symbol_list)} stocks left...")
            else:
                print(f"Retry succeeded: {symbol} exported")
            return failure_list
        except:
            error_str = symbol
            attempt += 1
            if attempt <= max_try:
                print(f"API error - {error_str}. Re-try in 60 sec: {max_try - attempt} "
                      f"attempts left...")
                time.sleep(60)
                download_yq(error_str, attempt, failure_list)
            else:
                failure_list.append(error_str)
                print(f'API error - {error_str} failed after {max_try} attempts')
                return failure_list
            # restart counting at next ticker
            attempt = 0
            continue


def format_data(df):
    """ Return the formatted one row annual statement dataframe.

    :param df: Dataframe
    :return: formatted Dataframe
    """

    if len(df.columns) <= 1:
        df.columns = [0]
        return df
    else:
        col = df.index.values.tolist()
        first = df.iloc[:, :1]
        first.columns = [0]
        second = df.iloc[:, 1:2]
        second.columns = [0]
        second.index = [s + "_-1" for s in col]
        return pd.concat([second, first])


def get_quote(symbol):
    """ Get the updated quote from yfinance fast_info

    :param symbol: string stock symbol
    :return: quote given option
    """

    company = Ticker(symbol)

    price = company.financial_data['currentPrice']
    price_currency = company.price['currency']
    report_currency = company.financial_data['financialCurrency']

    return price, price_currency, report_currency


def get_forex(buy, sell):
    """get exchange rate, buy means ask and sell means bid

    :param buy: report currency
    :param sell: price currency
    """

    try:
        if buy != sell:
            c = CurrencyRates()
            return c.get_rate(buy, sell)
        elif buy == "HKD" and sell == "MOP":
            return 0.97  # MOP to HKD not available
        else:
            return 1
    except:
        print("fx_rate error: Currency Rates Not Available")
        # will return None


class YqData(Stock):
    """Retrieves the data from yahooquery"""

    def __init__(self, symbol):
        """
        :param symbol: string ticker of the stock
        """
        super().__init__(symbol)
        try:
            self.stock_data = Ticker(self.symbol)
        except KeyError:
            print("Check your stock ticker")
        self.load_attributes()

    def load_attributes(self):

        self.name = self.stock_data.quote_type[self.symbol]['shortName']
        self.sector = self.stock_data.asset_profile[self.symbol]['sector']
        self.price = [self.stock_data.financial_data[self.symbol]['currentPrice'],
                      self.stock_data.price[self.symbol]['currency']]
        self.exchange = self.stock_data.quote_type[self.symbol]['exchange']
        self.shares = self.stock_data.key_stats[self.symbol]['sharesOutstanding']
        self.report_currency = self.stock_data.financial_data[self.symbol]['financialCurrency']
        self.annual_bs = self.get_balance_sheet("annual")
        self.quarter_bs = self.get_balance_sheet("quarterly")
        self.is_df = self.get_income_statement()
        self.cf_df = self.get_cash_flow()
        # left most column contains the most recent data
        self.most_recent_quarter = self.quarter_bs.columns[0]  # .strftime("%Y-%m-%d")
        try:
            self.last_dividend = -int(self.cf_df.loc['CashDividendsPaid'][0]) / self.shares
        except ZeroDivisionError:
            self.last_dividend = 0
        try:
            self.buyback = -int(self.cf_df.loc['RepurchaseOfCapitalStock'][0]) / self.shares
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
                    'LongTermCapitalLeaseObligation', 'TotalEquityGrossMinorityInterest', 'CommonStockEquity',
                    'CashAndCashEquivalents', 'OtherShortTermInvestments', 'InvestmentProperties',
                    'LongTermEquityInvestment', 'InvestmentinFinancialAssets', 'NetPPE']

        if option == "annual":
            # reverses the dataframe with .iloc[:, ::-1]
            balance_sheet = self.stock_data.balance_sheet(trailing=False).set_index('asOfDate').T.iloc[:, ::-1]
        else:
            balance_sheet = self.stock_data.balance_sheet(frequency="q", trailing=False).set_index('asOfDate') \
                                .T.iloc[:, ::-1]
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
        dummy = {"Dummy": [None, None, None, None, None, None]}
        cf_index = ['OperatingCashFlow', 'InvestingCashFlow', 'FinancingCashFlow',
                    'CashDividendsPaid', 'RepurchaseOfCapitalStock', 'EndCashPosition']
        dummy_df = pd.DataFrame(dummy, index=cf_index)
        clean_cf = dummy_df.join(cash_flow)
        cf_df = clean_cf.loc[cf_index]
        # Ending of Cleaning: drop the dummy column after join
        cf_df.drop('Dummy', inplace=True, axis=1)

        return cf_df.fillna(0)

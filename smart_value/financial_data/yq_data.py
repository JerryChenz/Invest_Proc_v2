from smart_value.stock import *
from smart_value.tools.stock_screener import *
from yahooquery import Ticker
from forex_python.converter import CurrencyRates
import time

cwd = pathlib.Path.cwd().resolve()
screener_folder = cwd / 'financial_models' / 'Opportunities' / 'Screener'
json_dir = screener_folder / 'data'


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

    stock_data = Ticker(symbols)
    symbol_list = symbols.split(" ")
    while symbol_list:
        symbol = symbol_list.pop(0)  # pop from the beginning
        try:
            # introductory information
            info = {'shortName': stock_data.quote_type[symbol]['shortName'],
                    'sector': stock_data.asset_profile[symbol]['sector'],
                    'industry': stock_data.asset_profile[symbol]['industry'],
                    'market': stock_data.quote_type[symbol]['exchange'],
                    'sharesOutstanding': stock_data.key_stats[symbol]['sharesOutstanding'],
                    'financialCurrency': stock_data.financial_data[symbol]['financialCurrency'],
                    'source': 'yq_data'}
            info_df = pd.DataFrame.from_dict(info, orient='index').T

            # Balance Sheet
            bs_df = stock_data.balance_sheet(frequency="q", trailing=False).set_index('asOfDate').T.iloc[:, ::-1]
            info_df['mostRecentQuarter'] = bs_df.columns[0]
            bs_df = format_data(bs_df)

            # Income statement
            is_df = stock_data.income_statement(trailing=False).set_index('asOfDate').T.iloc[:, ::-1]
            info_df['lastFiscalYearEnd'] = is_df.columns[0]
            is_df = format_data(is_df)
            # # Cash Flow statement
            cf_df = stock_data.cash_flow(trailing=False).set_index('asOfDate').T.iloc[:, ::-1]
            cf_df = format_data(cf_df)
            # Create one big one-row stock_df by using side-by-side merge
            stock_df = pd.concat([info_df.T, bs_df, is_df, cf_df])
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

    price = None
    price_currency = None
    report_currency = None

    tries = 2
    for i in range(tries):
        try:
            company = Ticker(symbol)

            price = company.financial_data[symbol]['currentPrice']
            price_currency = company.price[symbol]['currency']
            report_currency = company.financial_data[symbol]['financialCurrency']
        except:  # random API error
            if i < tries - 1:  # i is zero indexed
                wait = 60
                print(f"Possible API error, wait {wait} seconds before retry...")
                time.sleep(wait)
                continue
            else:
                return price, price_currency, report_currency
        else:
            return price, price_currency, report_currency


def update_data(data):
    """Update the price and currency info"""

    ticker = data.index.values.tolist()[0]
    quote = get_quote(ticker)
    data['price'] = quote[0]
    data['priceCurrency'] = quote[1]
    # Forex
    data['fxRate'] = get_forex(data['financialCurrency'].values[:1][0], data['priceCurrency'].values[:1][0])

    return data


def clean_data(df):
    """Clean up the dataframe"""

    # Exclude the rows that has missing financial data
    df = df[df['TotalAssets'].notna() & df['financialCurrency'].notna()]
    # print(df.to_string())
    # Preparing the data
    df['lastFiscalYearEnd'] = pd.to_datetime(df['lastFiscalYearEnd'])
    df['lastDividend'] = - df['CashDividendsPaid'] / df['sharesOutstanding']
    df['lastBuyback'] = - df['RepurchaseOfCapitalStock'] / df['sharesOutstanding']
    df['NoncurrentLiability'] = df['TotalAssets'] - df['TotalEquityGrossMinorityInterest']
    df['NoncurrentLiability_-1'] = df['TotalAssets_-1'] - df['TotalEquityGrossMinorityInterest_-1']
    df['NoncommonInterest'] = df['TotalEquityGrossMinorityInterest'] - df['CommonStockEquity']
    df['NoncommonInterest_-1'] = df['TotalEquityGrossMinorityInterest_-1'] - df['CommonStockEquity_-1']

    df = df.fillna(0)
    df = df[
        ['shortName', 'sector', 'industry', 'market', 'priceCurrency', 'sharesOutstanding',
         'financialCurrency', 'lastFiscalYearEnd', 'mostRecentQuarter', 'lastDividend', 'lastBuyback',
         'TotalAssets', 'CurrentAssets', 'CurrentLiabilities',
         'TotalAssets_-1', 'CurrentAssets_-1', 'CurrentLiabilities_-1',
         'CashAndCashEquivalents', 'OtherShortTermInvestments',
         'CashAndCashEquivalents_-1', 'OtherShortTermInvestments_-1',
         'CurrentDebtAndCapitalLeaseObligation', 'CurrentCapitalLeaseObligation',
         'CurrentDebtAndCapitalLeaseObligation_-1', 'CurrentCapitalLeaseObligation_-1',
         'LongTermDebtAndCapitalLeaseObligation', 'LongTermCapitalLeaseObligation',
         'LongTermDebtAndCapitalLeaseObligation_-1', 'LongTermCapitalLeaseObligation_-1',
         'TotalEquityGrossMinorityInterest', 'CommonStockEquity',
         'TotalEquityGrossMinorityInterest_-1', 'CommonStockEquity_-1',
         'InvestmentProperties', 'LongTermEquityInvestment', 'InvestmentinFinancialAssets',
         'InvestmentProperties_-1', 'LongTermEquityInvestment_-1', 'InvestmentinFinancialAssets_-1',
         'NetPPE', 'TotalRevenue', 'CostOfRevenue', 'SellingGeneralAndAdministration',
         'NetPPE_-1', 'TotalRevenue_-1', 'CostOfRevenue_-1', 'SellingGeneralAndAdministration_-1',
         'InterestExpense', 'NetIncomeCommonStockholders',
         'InterestExpense_-1', 'NetIncomeCommonStockholders_-1',
         'OperatingCashFlow', 'InvestingCashFlow', 'FinancingCashFlow', 'EndCashPosition',
         'OperatingCashFlow_-1', 'InvestingCashFlow_-1', 'FinancingCashFlow_-1', 'EndCashPosition_-1']]

    return df


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
        try:
            self.last_dividend = -int(self.cf_df.loc['CashDividendsPaid'][0]) / self.shares
        except ZeroDivisionError:
            self.last_dividend = 0
        try:
            self.buyback = -int(self.cf_df.loc['RepurchaseOfCapitalStock'][0]) / self.shares
        except ZeroDivisionError:
            self.buyback = 0
        # left most column contains the most recent data
        self.last_fy = self.annual_bs.columns[0]
        self.most_recent_quarter = self.quarter_bs.columns[0]

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
